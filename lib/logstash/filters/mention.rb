# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"

# The clone filter is for duplicating events.
# A clone will be made for each type in the clone list.
# The original event is left unchanged.
class LogStash::Filters::Mention < LogStash::Filters::Base

  config_name "mention"

  #accept an argument of field name, and type and set them in the filter
  # array object name in event['doc'] to get the data
  config :mention_field, :validate => :string, :default => "mentioned_apps_objects"

  #set event type
  config :mention_type, :validate => :string, :default => "wordpressDoc"

  # A new clone will be created with the given type for each type in this list.
  #config :clones, :validate => :array, :default => []



  public
  def register
    # Nothing to do
  end

  public
  def filter(event)
    return unless filter?(event)


    if event["doc"] and event["doc"][@mention_field].is_a?(Array)

      #remove duplicates
      event["doc"][@mention_field] = event["doc"][@mention_field].inject([]) { |result,h| result << h unless result.include?(h); result }
      event["doc"]["mentioned_apps"] = event["doc"]["mentioned_apps"].uniq

      event["doc"][@mention_field].each_with_index do |object, index|
        next unless object.is_a?(Hash)

        #clone event
        e = event.clone
        mention_id = object['identifier'] || object['trackId']

        #puts "size: " + event["doc"][@mention_field].size.to_s
        #puts "size2: " + event["doc"]["mentioned_apps"].size.to_s
        #puts "array: " + event["doc"]["mentioned_apps"].to_s

        #puts "index: " + index.to_s
        #puts event["@metadata"]["_id"].to_s + "_" + mention_id

        if !mention_id.nil?

          e["type"] = @mention_type
          e["@metadata"]["_id"] = event["@metadata"]["_id"].to_s + "_" + mention_id
          e["doc"][@mention_field] = [object] #empty the mention array field
          e["doc"]["doc_type"] = @mention_field #set doc_type to the passed doc_type

          #testing
          #(e['tags'] ||= []) << e["@metadata"]["_id"]

          @logger.debug("Created a mention event", :event => e)
          filter_matched(e)
          #e.remove("doc")
          yield e #send new event

          #create second event of type 'appDoc' to set 'isMentioned' = true
          e2 = LogStash::Event.new()
          e2["type"] = "appDoc"
          e2["@metadata"]["_id"] = mention_id
          e2["isMentioned"] = true
          #(e2['tags'] ||= []) << 'mentioned'
          filter_matched(e2)
          yield e2 #send second event

        end
      end
    end


    # @clones.each do |type|
    #   clone = event.clone
    #   clone["type"] = type
    #   filter_matched(clone)
    #   @logger.debug("Cloned event", :clone => clone, :event => event)
    #
    #   # Push this new event onto the stack at the LogStash::FilterWorker
    #   yield clone
    # end
  end

end # class LogStash::Filters::Mention