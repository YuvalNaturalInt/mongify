require 'cassandra'

module Mongify
  module Database
    #
    # No Sql Connection configuration
    #
    # Basic format should look something like this:
    #
    #   no_sql_connection {options} do
    #     adapter   "mongodb"
    #     host      "localhost"
    #     database  "my_database"
    #   end
    #
    # Possible attributes:
    #   adapter
    #   host
    #   database
    #   username
    #   password
    #   port
    #
    # Options:
    #   :force => true       # This will force a database drop before processing
    # <em>You're also able to set attributes via the options</em>
    #
    class CassandraConnection < Mongify::Database::NoSqlConnection


      #Required fields for a no sql connection
      REQUIRED_FIELDS = %w{host database}

      def initialize(options={})
        super options
        @options = options
        @keyspace = @database
        @all_tables = {}

        adapter 'cassandra-driver' if adapter.nil? || adapter.downcase == "cassandra-driver"
      end

      # Sets and/or returns a adapter
      # It takes care of renaming adapter('mongo') to 'mongodb'
      def adapter(name=nil)
        super(name)
      end

      # Returns a connection string that can be used to build a Mongo Connection
      # (Currently this isn't used due to some issue early on in development)
      def connection_string
        "#{@adapter}://#{@host}#{":#{@port}" if @port}"
      end

      # Returns true or false depending if the given attributes are present and valid to make up a
      # connection to a mongo server
      def valid?
        super && @database.present?
      end

      # Returns true if :force was set to true
      # This will force a drop of the database upon connection
      def forced?
        !!@options['force']
      end

      # Sets up a connection to the database
      def setup_connection_adapter

          connection = Cassandra.connect(@options)
            keyspace_exists = false

            keyspace = 'system'
            session  = connection.connect(keyspace)

            #connection.add_auth(database, username, password) if username && password
            future = session.execute("SELECT keyspace_name, columnfamily_name FROM schema_columnfamilies where keyspace_name = '#{@database}'") # fully asynchronous api
            future.rows.each do |row|

              if(row['keyspace_name'] == @database)
                keyspace_exists = true
                @all_tables[row['columnfamily_name']] = true
                puts "The keyspace #{row['keyspace_name']} has a table called #{row['columnfamily_name']}"
              end

            end
            unless(keyspace_exists)
              keyspace_definition =
                  "CREATE KEYSPACE #{@database}
                WITH replication = {
                  'class': 'SimpleStrategy',
                  'replication_factor': 3
                }"
              begin
                session.execute(keyspace_definition)
              rescue
              end
            end

        connection
      end

      # Returns a mongo connection
      # NOTE: If forced? is true, the first time a connection is made, it will ask to drop the
      # database before continuing
      def connection
        return @connection if @connection
        @connection = setup_connection_adapter
        @connection
      end

      # Returns true or false depending if we have a connection to a mongo server
      def has_connection?
        return true
      end

      # Returns the database from the connection
      def db

        unless(@db)
          @db = connection.connect(database)

        end

        @db

      end

      # Returns a hash of all the rows from the database of a given collection
      def select_rows(collection)
        query = "SELECT * FROM #{collection}"
        db.execute(query)
      end

      def select_by_query(collection, query)
        query = "SELECT * FROM #{collection} WHERE #{get_hash_query(query, 'AND')}"
        db.execute(query)
      end

      def create_table(colleciton_name, columns)
        unless(@all_tables[colleciton_name])
          column_defenitions = []
          primary_keys = []
            columns.each do |column|
            name = column.sql_name
            type = column.type
            if(type == :key)
              primary_keys << name
              type = 'uuid'
            end
            type = 'timestamp' if type == :datetime
            type = 'text' if type == :string
            column_defenitions << "#{name} #{type.upcase}"
          end

          table_definition = "CREATE TABLE #{colleciton_name} (#{column_defenitions.join(', ')}, PRIMARY KEY(#{primary_keys.join(', ')}))"
          begin
           db.execute(table_definition)
          rescue Exception => e
            puts e
          end
        end
      end

      # Inserts into the collection a given row
      def insert_into(colleciton_name, row)

        if(row.kind_of?(Array))
          row.each do |single_row|
            insert_single_row(colleciton_name, single_row)
          end
        else
          insert_single_row(colleciton_name, row)
        end

      end

      def insert_single_row(colleciton_name, row)
        fields = row.keys.join(",")
        values = row.values.map do |value|
          value = value.iso8601 if value.is_a? Time
          value = "'#{value}'" unless value.is_a? Numeric

          value
        end. join(",")
        query = "INSERT INTO #{colleciton_name} (#{fields}) VALUES (#{values})"
        db.execute(query)
      end

      # Updates a collection item with a given ID with the given attributes
      def update(colleciton_name, id, attributes)
        values = get_hash_query(attributes)
        query = "UPDATE #{colleciton_name} SET #{values} WHERE id = '#{id}'"
        db.execute(query)
      end

      # Upserts into the collection a given row
      def upsert(collection_name, row)
        # We can't use the save method of the Mongo collection
        # The reason is that it detects duplicates using _id
        # but we should detect it using pre_mongified_id instead
        # because in the case of sync, same rows are identified by their original sql ids
        #
        # db[collection_name].save(row)

        if row.has_key?(:pre_mongified_id) || row.has_key?('pre_mongified_id')
          id = row[:pre_mongified_id] || row['pre_mongified_id']
          duplicate = find_one(collection_name, {"pre_mongified_id" => id})
          if duplicate
            update(collection_name, duplicate[:_id] || duplicate["_id"], row)
          else
            insert_into(collection_name, row)
          end
        else
          # no pre_mongified_id, fallback to the upsert method of Mongo
          insert_into(collection_name, row)
          #db[collection_name].save(row)
        end
      end

      # Finds one item from a collection with the given query
      def find_one(collection_name, query)
        search_query = get_hash_query(query)
        search_query = "SELECT * FROM #{collection_name} WHERE #{search_query}"
        db.execute(search_query)
      end

      # Returns a row of a item from a given collection with a given pre_mongified_id
      def get_id_using_pre_mongified_id(colleciton_name, pre_mongified_id)
        find_one(colleciton_name, {'pre_mongified_id' => pre_mongified_id}).try(:[], '_id')
      end

      # Removes pre_mongified_id from all records in a given collection
      def remove_pre_mongified_ids(collection_name)
        drop_mongified_index(collection_name)
        #db[collection_name].update({}, { '$unset' => { 'pre_mongified_id' => 1} }, :multi => true)
      end

      # Removes pre_mongified_id from collection
      # @param [String] collection_name name of collection to remove the index from
      # @return True if successful
      # @raise MongoDBError if index isn't found
      def drop_mongified_index(collection_name)
        #db[collection_name].drop_index('pre_mongified_id_1') if db[collection_name].index_information.keys.include?("pre_mongified_id_1")
      end

      # Creates a pre_mongified_id index to ensure
      # speedy lookup for collections via the pre_mongified_id
      def create_pre_mongified_id_index(collection_name)
        #db[collection_name].create_index([['pre_mongified_id', Mongo::ASCENDING]])
      end

      # Asks user permission to drop the database
      # @return true or false depending on user's response
      def ask_to_drop_database
        if UI.ask("Are you sure you want to drop #{database} database?")
          drop_database
        end
      end

      #######
      private
      #######

      # Drops the mongodb database
      def drop_database
        #connection.drop_database(database)
      end

      def get_hash_query(query, delimiter = ',')
        query.map { |key, value| "#{key} = '#{value}'"}.join(delimiter)
      end



    end
  end
end
