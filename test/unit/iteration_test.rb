# frozen_string_literal: true

require "test_helper"
require "sorbet-runtime"

class JobIterationTest < IterationUnitTest
  class JobWithNoMethods < ActiveJob::Base
    include JobIteration::Iteration
  end

  class JobWithRightMethods < ActiveJob::Base
    include JobIteration::Iteration
    def build_enumerator(_params, cursor:)
      enumerator_builder.build_times_enumerator(2, cursor: cursor)
    end

    def each_iteration(*)
    end
  end

  class JobWithRightMethodsButWithSorbetSignatures < ActiveJob::Base
    extend T::Sig
    include JobIteration::Iteration

    sig { params(_params: T.untyped, cursor: T.untyped).returns(T::Enumerator[T.untyped]) }
    def build_enumerator(_params, cursor:)
      enumerator_builder.build_times_enumerator(2, cursor: cursor)
    end

    sig { params(product: T.untyped, params: T.untyped).void }
    def each_iteration(product, params)
    end
  end

  class JobWithRightMethodsButMissingCursorKeywordArgument < ActiveJob::Base
    include JobIteration::Iteration
    def build_enumerator(params, cursor)
      enumerator_builder.active_record_on_records(
        Product.where(id: params[:id]),
        cursor: cursor,
      )
    end

    def each_iteration(product, params)
    end
  end

  class JobWithRightMethodsUsingSplatInTheArguments < ActiveJob::Base
    include JobIteration::Iteration
    def build_enumerator(*)
    end

    def each_iteration(*)
    end
  end

  class JobWithRightMethodsUsingDefaultKeywordArgument < ActiveJob::Base
    include JobIteration::Iteration
    def build_enumerator(params, cursor: nil)
    end

    def each_iteration(*)
    end
  end

  class InvalidCursorJob < ActiveJob::Base
    include JobIteration::Iteration
    def each_iteration(*)
      return if Gem::Version.new(JobIteration::VERSION) < Gem::Version.new("2.0")
      raise "Cursor invalid. This should never run!"
    end
  end

  class JobWithTimeCursor < InvalidCursorJob
    def build_enumerator(cursor:)
      [["item", cursor || Time.now]].to_enum
    end
  end

  class JobWithSymbolCursor < InvalidCursorJob
    def build_enumerator(cursor:)
      [["item", cursor || :symbol]].to_enum
    end
  end

  class JobWithActiveRecordCursor < InvalidCursorJob
    def build_enumerator(cursor:)
      [["item", cursor || Product.first]].to_enum
    end
  end

  class JobWithStringSubclassCursor < InvalidCursorJob
    StringSubClass = Class.new(String)

    def build_enumerator(cursor:)
      [["item", cursor || StringSubClass.new]].to_enum
    end
  end

  class JobWithBasicObjectCursor < InvalidCursorJob
    def build_enumerator(cursor:)
      [["item", cursor || BasicObject.new]].to_enum
    end
  end

  class JobWithComplexCursor < ActiveJob::Base
    include JobIteration::Iteration
    def build_enumerator(cursor:)
      [[
        "item",
        cursor || [{
          "string" => "abc",
          "integer" => 123,
          "float" => 4.56,
          "booleans" => [true, false],
          "null" => nil,
        }],
      ]].to_enum
    end

    def each_iteration(*)
    end
  end

  def test_jobs_that_define_build_enumerator_and_each_iteration_will_not_raise
    push(JobWithRightMethods, "walrus" => "best")
    work_one_job
  end

  def test_jobs_that_define_build_enumerator_and_each_iteration_with_sigs_will_not_raise
    push(JobWithRightMethodsButWithSorbetSignatures, "walrus" => "best")
    work_one_job
  end

  def test_jobs_that_pass_splat_argument_to_build_enumerator_will_not_raise
    push(JobWithRightMethodsUsingSplatInTheArguments, {})
    work_one_job
  end

  def test_jobs_that_pass_default_keyword_argument_to_build_enumerator_will_not_raise
    push(JobWithRightMethodsUsingDefaultKeywordArgument, {})
    work_one_job
  end

  def test_jobs_that_do_not_define_build_enumerator_or_each_iteration_raises
    assert_raises(ArgumentError) do
      push(JobWithNoMethods)
    end
  end

  def test_jobs_that_defines_methods_but_do_not_declare_cursor_as_keyword_argument_raises
    assert_raises(ArgumentError) do
      push(JobWithRightMethodsButMissingCursorKeywordArgument, id: 1)
    end
  end

  def test_that_it_has_a_version_number
    refute_nil(::JobIteration::VERSION)
  end

  def test_that_the_registered_method_added_hook_calls_super
    methods_added = []

    hook_module = Module.new do
      define_method(:method_added) do |name|
        methods_added << name
      end
    end

    Class.new(ActiveJob::Base) do
      # The order below is important.
      # We want the Hook Module to add the `method_added` first
      # and then `Iteration` to override it. That means that if
      # the `method_added` in `Iteration` does not call `super`
      # `foo` will **not** be in the `methods_added` list.
      extend hook_module
      include JobIteration::Iteration

      def foo
      end
    end

    assert_includes(methods_added, :foo)
  end

  def test_jobs_using_time_cursor_is_deprecated
    push(JobWithTimeCursor)
    assert_cursor_deprecation_warning { work_one_job }
  end

  def test_jobs_using_active_record_cursor_is_deprecated
    refute_nil(Product.first)
    push(JobWithActiveRecordCursor)
    assert_cursor_deprecation_warning { work_one_job }
  end

  def test_jobs_using_symbol_cursor_is_deprecated
    push(JobWithSymbolCursor)
    assert_cursor_deprecation_warning { work_one_job }
  end

  def test_jobs_using_string_subclass_cursor_is_deprecated
    push(JobWithStringSubclassCursor)
    assert_cursor_deprecation_warning { work_one_job }
  end

  def test_jobs_using_basic_object_cursor_is_deprecated
    push(JobWithBasicObjectCursor)
    assert_cursor_deprecation_warning { work_one_job }
  end

  def test_jobs_using_complex_but_serializable_cursor_is_not_deprecated
    push(JobWithComplexCursor)
    assert_no_cursor_deprecation_warning do
      work_one_job
    end
  end

  private

  def assert_cursor_deprecation_warning
    original_behaviour = JobIteration::Deprecation.behavior
    warning_count = 0
    prefix = <<~PREFIX
      DEPRECATION WARNING: Cursor must be composed of objects capable of built-in (de)serialization:
        Strings, Integers, Floats, Arrays, Hashes, true, false, or nil.
      #{ActiveJob::Base.queue_adapter.enqueued_jobs.first.fetch("job_class")}#build_enumerator's Enumerator provided:
    PREFIX
    JobIteration::Deprecation.behavior = lambda do |message, _callstack, deprecation_horizon, gem_name|
      warning_count += 1
      suffix = "This will raise starting in version #{deprecation_horizon} of #{gem_name}!"
      assert_match(/\A#{Regexp.escape(prefix)}  .+\n#{Regexp.escape(suffix)}/, message)
    end
    yield
    assert_equal(1, warning_count, "expected deprecation warning")
  ensure
    JobIteration::Deprecation.behavior = original_behaviour
  end

  def assert_no_cursor_deprecation_warning
    original_behaviour = JobIteration::Deprecation.behavior
    JobIteration::Deprecation.behavior = lambda do |message, _callstack, _deprecation_horizon, _gem_name|
      flunk("Expected no deprecation warning: #{message}")
    end
    yield
  ensure
    JobIteration::Deprecation.behavior = original_behaviour
  end

  def push(job, *args)
    job.perform_later(*args)
  end

  def work_one_job
    job = ActiveJob::Base.queue_adapter.enqueued_jobs.pop
    ActiveJob::Base.execute(job)
  end
end
