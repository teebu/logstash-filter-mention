# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"

# mention filter will create events for every mention
# and a second event with the type 'appDoc' will be created with a single value, 'isMentioned' = true
# The original event is left unchanged.
class LogStash::Filters::Mention < LogStash::Filters::Base

  config_name "mention"

  #accept an argument of field name, and type and set them in the filter
  # array object name in event['doc'] to get the data
  config :mention_field, :validate => :string, :default => "mentioned_apps_objects"

  #set event type
  config :mention_type, :validate => :string, :default => "wordpressPost"

  # Example:
  #
  # filter {
  #
  #   mention {
  #     mention_field => "mentioned_apps_objects"
  #     mention_type => "test"
  #     add_tag => ["sample_tag"]
  #   }
  # }


  public
  def register
    # Nothing to do
  end

  public
  def filter(event)
    return unless filter?(event)

    if event["doc"] and event["doc"][@mention_field].is_a?(Array)
      event["doc"][@mention_field].each_with_index do |object, index|
        next unless object.is_a?(Hash)

        #clone event
        e = event.clone
        mention_id = object['identifier'] || object['trackId']

        #puts "size: " + event["doc"][@mention_field].size.to_s
        #puts "index: " + index.to_s
        #puts event["@metadata"]["_id"].to_s + "_" + mention_id

        if !mention_id.nil?

          e["type"] = @mention_type
          e["@metadata"]["_id"] = event["@metadata"]["_id"].to_s + "_" + mention_id
          e["doc"][@mention_field] = [object] #empty the mention array field

          #testing
          #(e['tags'] ||= []) << e["@metadata"]["_id"]

          @logger.debug("Created a mention event", :event => e)
          filter_matched(e)
          #e.remove("doc")
          yield e #send new event

          #create second event of type 'appDoc' to set 'isMentioned' = true
          e2 = LogStash::Event.new()
          e2["type"] = "appDoc"
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
