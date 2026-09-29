# frozen_string_literal: true

module WorkloadOrchestrator
  # Computes deterministic v0.3 assignments from durable job state and the
  # current provider-neutral registry view. It does not dispatch work.
  class DynamicScheduler
    Assignment = Struct.new(:job, :worker, keyword_init: true)
    Edge = Struct.new(:to, :reverse, :capacity, :cost, :tag, keyword_init: true)

    class Network
      def initialize(size)
        @edges = Array.new(size) { [] }
      end

      def add_edge(from, to, capacity, cost, tag: nil)
        forward = Edge.new(
          to: to, reverse: @edges[to].length, capacity: capacity, cost: cost, tag: tag
        )
        reverse = Edge.new(
          to: from, reverse: @edges[from].length, capacity: 0, cost: -cost
        )
        @edges[from] << forward
        @edges[to] << reverse
      end

      def maximum_flow(source, sink)
        while (path = shortest_path(source, sink))
          augment(path, source, sink)
        end
        @edges.flatten.filter_map { |edge| edge.tag if edge.tag && edge.capacity.zero? }
      end

      private

      def shortest_path(source, sink)
        distances = Array.new(@edges.length)
        previous = Array.new(@edges.length)
        distances[source] = 0
        (@edges.length - 1).times do
          changed = relax_edges(distances, previous)
          break unless changed
        end
        distances[sink] && previous
      end

      def relax_edges(distances, previous)
        changed = false
        @edges.each_with_index do |edges, from|
          next unless distances[from]

          edges.each_with_index do |edge, index|
            next unless edge.capacity.positive?

            candidate = distances[from] + edge.cost
            next if distances[edge.to] && distances[edge.to] <= candidate

            distances[edge.to] = candidate
            previous[edge.to] = [from, index]
            changed = true
          end
        end
        changed
      end

      def augment(previous, source, sink)
        node = sink
        until node == source
          from, index = previous.fetch(node)
          edge = @edges.fetch(from).fetch(index)
          edge.capacity -= 1
          @edges.fetch(node).fetch(edge.reverse).capacity += 1
          node = from
        end
      end
    end
    private_constant :Edge, :Network

    attr_reader :plan, :store, :matcher

    def initialize(plan:, store:, matcher: CapabilityMatcher.new(plan: plan))
      raise Error, "dynamic scheduling requires wlo-execution-plan/v0.3" unless plan.priority_scheduling?

      @plan = plan
      @store = store
      @matcher = matcher
    end

    def assignments(workers:, current_workers: workers)
      metadata = plan.jobs.to_h { |job| [job.id, store.metadata_for(job)] }
      ready_workers = ready_workers!(workers)
      current_workers = current_workers!(current_workers)
      idle_workers = idle_workers!(ready_workers, current_workers, metadata)
      jobs = eligible_jobs(metadata)
      capacities = pool_capacities(metadata, jobs)
      graph = compatibility_graph(jobs, idle_workers, capacities)
      minimum_cost_assignments(jobs, idle_workers, graph, capacities).freeze
    end

    private

    def ready_workers!(workers)
      rows = Array(workers)
      raise Error, "dynamic scheduling requires RegistryWorker entries" unless rows.all?(RegistryWorker)

      ready = rows.select(&:ready?).sort_by(&:execution_identity)
      ensure_unique_identities!(ready)
      ready
    end

    def current_workers!(workers)
      rows = Array(workers)
      raise Error, "dynamic scheduling requires RegistryWorker entries" unless rows.all?(RegistryWorker)

      rows.sort_by(&:execution_identity).tap { |ordered| ensure_unique_identities!(ordered) }
    end

    def ensure_unique_identities!(workers)
      identities = workers.map(&:execution_identity)
      raise Error, "dynamic worker execution identities must be unique" unless identities.uniq == identities
    end

    def idle_workers!(workers, current_workers, metadata)
      running = metadata.filter_map do |job_id, row|
        next unless row&.fetch("status") == "running"

        DynamicWorkerBinding.from_metadata(row).tuple
      rescue Error => e
        raise Error, "running dynamic job #{job_id.inspect} has an invalid worker binding: #{e.message}"
      end
      raise Error, "one dynamic worker is bound to multiple running jobs" unless running.uniq == running

      current = current_workers.map(&:execution_identity)
      missing = running.reject { |identity| current.include?(identity) }
      unless missing.empty?
        raise Error, "running dynamic worker binding is absent or replaced: #{missing.first.inspect}"
      end

      workers.reject { |worker| running.include?(worker.execution_identity) }
    end

    def eligible_jobs(metadata)
      plan.jobs.select do |job|
        row = metadata.fetch(job.id)
        pending = row.nil? || row.fetch("status") == "pending"
        pending && dependencies_terminal?(job, metadata)
      end.sort_by(&:priority_key)
    end

    def dependencies_terminal?(job, metadata)
      job.depends_on_job_ids.all? do |job_id|
        row = metadata.fetch(job_id)
        row && ExecutionStore::TERMINAL_JOB_STATUSES.include?(row.fetch("status"))
      end
    end

    def pool_capacities(metadata, jobs)
      running = Hash.new(0)
      plan.jobs.each do |job|
        running[job.pool_id] += 1 if metadata.fetch(job.id)&.fetch("status") == "running"
      end
      eligible = jobs.group_by(&:pool_id)
      plan.pools.to_h do |pool|
        limit = pool.max_concurrency
        if limit && running[pool.id] > limit
          raise Error, "pool #{pool.id.inspect} has more running jobs than its concurrency ceiling"
        end

        available = limit ? limit - running[pool.id] : eligible.fetch(pool.id, []).length
        [pool.id, [available, 0].max]
      end
    end

    def compatibility_graph(jobs, workers, capacities)
      jobs.to_h do |job|
        compatible = if capacities.fetch(job.pool_id).zero?
                       []
                     else
                       workers.select { |worker| matcher.match_job(job: job, worker: worker).compatible? }
                     end
        [job, compatible]
      end
    end

    def minimum_cost_assignments(jobs, workers, graph, capacities)
      candidates = jobs.select { |job| !graph.fetch(job).empty? && capacities.fetch(job.pool_id).positive? }
      return [] if candidates.empty? || workers.empty?

      nodes = network_nodes(candidates, workers)
      network = Network.new(nodes.fetch(:size))
      add_pool_edges(network, nodes, candidates, capacities)
      add_worker_edges(network, nodes, workers)
      add_assignment_edges(network, nodes, candidates, workers, graph)
      network.maximum_flow(nodes.fetch(:source), nodes.fetch(:sink)).sort_by do |assignment|
        assignment.job.priority_key
      end
    end

    def network_nodes(jobs, workers)
      next_node = 1
      pools = jobs.map(&:pool_id).uniq.sort.to_h { |id| [id, next_node].tap { next_node += 1 } }
      job_nodes = jobs.to_h { |job| [job, next_node].tap { next_node += 1 } }
      worker_nodes = workers.to_h { |worker| [worker, next_node].tap { next_node += 1 } }
      { source: 0, pools: pools, jobs: job_nodes, workers: worker_nodes,
        sink: next_node, size: next_node + 1 }
    end

    def add_pool_edges(network, nodes, jobs, capacities)
      jobs.group_by(&:pool_id).sort.each do |pool_id, pool_jobs|
        capacity = [capacities.fetch(pool_id), pool_jobs.length].min
        network.add_edge(nodes.fetch(:source), nodes.fetch(:pools).fetch(pool_id), capacity, 0)
        pool_jobs.each do |job|
          network.add_edge(nodes.fetch(:pools).fetch(pool_id), nodes.fetch(:jobs).fetch(job), 1, 0)
        end
      end
    end

    def add_worker_edges(network, nodes, workers)
      workers.each do |worker|
        network.add_edge(nodes.fetch(:workers).fetch(worker), nodes.fetch(:sink), 1, 0)
      end
    end

    def add_assignment_edges(network, nodes, jobs, workers, graph)
      worker_base = workers.length + 1
      job_scale = worker_base**jobs.length
      workers_by_rank = workers.each_with_index.to_h
      jobs.each_with_index do |job, index|
        place = jobs.length - index - 1
        job_benefit = (1 << place) * job_scale
        graph.fetch(job).each do |worker|
          worker_penalty = workers_by_rank.fetch(worker) * (worker_base**place)
          assignment = Assignment.new(job: job, worker: worker).freeze
          network.add_edge(
            nodes.fetch(:jobs).fetch(job), nodes.fetch(:workers).fetch(worker), 1,
            -job_benefit + worker_penalty, tag: assignment
          )
        end
      end
    end
  end
end
