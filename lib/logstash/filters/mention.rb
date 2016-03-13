# encoding: utf-8
require "logstash/filters/base"
require "logstash/namespace"

# The clone filter is for duplicating events.
# A clone will be made for each type in the clone list.
# The original event is left unchanged.
class LogStash::Filters::Mention < LogStash::Filters::Base

  config_name "mention"

  # A new clone will be created with the given type for each type in this list.
  #config :clones, :validate => :array, :default => []

  public
  def register
    # Nothing to do
  end

  public
  def filter(event)
    return unless filter?(event)

    if event["doc"] and event["doc"]["mentioned_apps_objects"].is_a?(Array)
      event["doc"]["mentioned_apps_objects"].each do |apps_object|

        e = LogStash::Event.new()
        e["type"] = "mention"
        e["post_id"] = event["_id"]
        apps_object.to_hash.each{|k,v| e[k] = v}
        @logger.debug("Created a mention event", :event => event)
        yield e

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
