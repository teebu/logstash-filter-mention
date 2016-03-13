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
  config :mention_type, :validate => :string, :default => "mention"

  # A new clone will be created with the given type for each type in this list.
  #config :clones, :validate => :array, :default => []

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
      event["doc"][@mention_field].each do |object|
        next unless object.is_a?(Hash)

        e = LogStash::Event.new()

        e["type"] = @mention_type
        e["post_id"] = event["@metadata"]["_id"]
        object.each{|k,v| e[k] = v}
        @logger.debug("Created a mention event", :event => e)
        filter_matched(e)
        yield e #send first event

        #create second event of type 'appDoc' to set 'isMentioned' = true
        mention_id = object['identifier'] || object['trackId']
        if !mentionID.nil?
          e2 = LogStash::Event.new()
          e2["type"] = "appDoc"
          e2["isMentioned"] = true
          e2.tags = ['mentioned']
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

end # class LogStash::Filters::Clone
