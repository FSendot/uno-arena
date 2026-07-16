# frozen_string_literal: true

require_relative "test_helper"

class ParseYamlTest < Minitest::Test
  def test_accepts_all_documents_in_a_yaml_stream
    Dir.mktmpdir do |dir|
      path = File.join(dir, "pipeline.yml")
      File.write(path, "spec:\n  inputs: {}\n---\nstages: [test]\n")

      _stdout, stderr, status = Open3.capture3("ruby", "ci/bin/parse-yaml.rb", path)

      assert status.success?, stderr
    end
  end

  def test_rejects_invalid_later_document
    Dir.mktmpdir do |dir|
      path = File.join(dir, "pipeline.yml")
      File.write(path, "spec:\n  inputs: {}\n---\nstages: [test\n")

      _stdout, _stderr, status = Open3.capture3("ruby", "ci/bin/parse-yaml.rb", path)

      refute status.success?
    end
  end
end
