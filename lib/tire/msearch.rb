module Tire
  module Search
    # Multi Search API
    # @see http://www.elasticsearch.org/guide/reference/api/multi-search.html
    class Msearch
      include Common

      # Create a new msearch instance
      #
      # @param [Hash] options which will be used to construct a URL query
      def initialize(options = {}, &block)
        @options = options
        @parts = []
        @path = '/_msearch'
        instance_eval(&block)
      end

      # Create a search part
      #
      # @param [String] index can be 'index' or 'index/type'
      # @param [Hash] options, can be mapping, search_type, preference and routing
      def search(index, options = {}, &block)
        searcher = Tire::Search::Search.new(index, &block)
        if index.include?('/')
          index, type = index.split('/', 2)
          searcher.filter('type', value: type)
        end
        @parts << [index, options.dup, searcher]
      end

      # The msearch body
      def to_json
        @parts.map do |(index, options, part)|
          header = options.merge(index: index)
          [header.to_json, part.to_json].join("\n")
        end.join("\n")
      end

      # Perform the msearch. The results are returned as Array<Tire::Results::Collection>
      #
      # @return [Tire::Search::Msearch] self
      def perform
        @response = Configuration.client.get(self.url + self.params, self.to_json)
        if @response.failure?
          STDERR.puts "[REQUEST FAILED] #{self.to_curl}\n"
          raise SearchRequestFailed, @response.to_s
        end
        @json     = MultiJson.decode(@response.body)
        @results  = @json['responses'].map {|response| Results::Collection.new(response, @options)}
        return self
      ensure
        logged
      end

    end
  end
end
