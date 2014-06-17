require "synapse/service_watcher/base"

require 'thread'

module Synapse
  class SerfWatcher < BaseWatcher

    def initialize(opts = {}, synapse)
      super(opts, synapse)

      @all_backups_except_one = false
      if opts['haproxy']['all_backups_except_one']
        @all_backups_except_one = opts['haproxy']['all_backups_except_one']
      end
    end

    def start
      @serf_members = '/dev/shm/serf_members.json'
      @cycle_delay = 1

      @last_ctime = 0
      @last_discover = 0
      @last_members_raw = ""
      @last_backends_s = ""

      @watcher = Thread.new do
        watch
      end
    end


# There is an edge case here:
# say that there have been several changes in the network in the last seconds,
# such as a massive rejoin after a network partition.
# The software might pick up a file dated from the beginning of the second and 
# it might then be updated at the end of the second, and it wouldn't be noticed
# by stat(2) because it only does second timestamps
#
# So I gave it some thought and came up with something: enforce doing a reread
# ten seconds later

    def watch
      until @should_exit
        begin
          ctime = File.stat(@serf_members).ctime.to_i

          if ctime > @last_ctime or (@last_discover < ctime + 10 and Time.new.to_i > ctime + 10)
            @last_ctime = ctime
            discover()
          end
        rescue => e
          log.warn "Error in watcher thread: #{e.inspect}"
          log.warn e.backtrace
        end

        sleep @cycle_delay
      end

      log.info "serf watcher exited successfully"
    end

    def stop
      Thread.kill(@watcher)
      # Must restart???
      log.info "kill watcher for serf"
    end

    #def ping?
    #  @zk.ping?
    #end

    # find the current backends at the discovery path; sets @backends
    def discover
      log.info "discovering backends for service #{@name}"
      @last_discover = Time.now.to_i

      new_backends = []

      # PUT A BEGIN HERE?
      members_raw = File.read @serf_members
      return if members_raw == @last_members_raw

      members = false

      begin
        members = JSON.parse(members_raw)
      rescue Exception => e
        log.info "exception parsing json #{e.inspect}"
        members = false
      end

      members = false unless members.is_a? Hash

      if members.has_key? 'members'
        members = members['members']
      else
        members = false
      end

      members = false unless members.is_a? Array

      new_backends = []

      if members
        # Now I do my pretty parsing

        # please note that because of
        # https://github.com/airbnb/smartstack-cookbook/blob/master/recipes/nerve.rb#L71
        # the name won't just be the name you gave but name_port. this allows a same
        # service to be on multiple ports of a same machine.

        members.each do |member|
          next unless member['status'] == 'alive'
          member['tags'].each do |tag,data|
            if tag =~ /^smart:#{@name}(|_[0-9]+)$/
              host,port = data.split ':'

              # Special trick
              # If we have a all_backups_except_one option
              # We use it here
              # It makes every server except the one we specify (typically the current one)
              # be the only one that doesn't have a 'backup' flag in haproxy. Useful for a
              # scenario where we have a lot of slave servers (ie. mysql, sphinx) that have
              # a local copy of data, and we prefer them, but want to fallback to the others
              # in case of a problem
              
              extra_haproxy_conf = ''

              if @all_backups_except_one
                if host != @all_backups_except_one
                  extra_haproxy_conf = 'backup'
                end
              end


              new_backends << {
                'name' => member['name'],
                'host' => host,
                'port' => port,
                'extra_haproxy_conf' => extra_haproxy_conf,
              }
              log.debug "discovered backend #{member['name']} at #{host}:#{port} for service #{@name}"
            end
          end
        end

        # and sort to compare
        new_backends.sort! { |a,b| a.to_s <=> b.to_s }


        new_backends_s = new_backends.to_s
        if new_backends_s == @last_backends_s
          # we got the same result as last time - no need to reconfigure
          log.info "serf members list for #{@name} returned identical results - not reconfiguring"
          return
        end
        @last_backends_s = new_backends_s
      end

      if new_backends.empty?
        if @default_servers.empty?
          log.warn "no backends and no default servers for service #{@name}; using previous backends: #{@backends.inspect}"
        else
          log.warn "no backends for service #{@name}; using default servers: #{@default_servers.inspect}"
          @backends = @default_servers
        end
      else
        log.info "discovered #{new_backends.length} backends for service #{@name}"
        @backends = new_backends
        @synapse.reconfigure!
      end
    end

    private
    #WTF is the use of this??
    def validate_discovery_opts
      raise ArgumentError, "invalid discovery method #{@discovery['method']}" \
        unless @discovery['method'] == 'serf'
    end

  end
end
