# frozen_string_literal: true

# Deterministic ClickHouse SQL statement splitter (Ruby stdlib only).
# Splits on semicolons that appear outside quoted text and comments so each
# statement can be POSTed as one ClickHouse HTTP request (multi-statement
# bodies are rejected with Code 62; multiquery=1 is unsupported here).

module ClickhouseSqlSplit
  class Error < StandardError; end

  module_function

  def split(sql)
    raise Error, "sql must be a String" unless sql.is_a?(String)

    statements = []
    buf = String.new
    i = 0
    n = sql.length
    state = :code

    while i < n
      ch = sql[i]
      nxt = i + 1 < n ? sql[i + 1] : nil

      case state
      when :code
        if ch == "-" && nxt == "-"
          buf << ch << nxt
          i += 2
          state = :line_comment
          next
        elsif ch == "/" && nxt == "*"
          buf << ch << nxt
          i += 2
          state = :block_comment
          next
        elsif ch == "'"
          buf << ch
          i += 1
          state = :single_quote
          next
        elsif ch == '"'
          buf << ch
          i += 1
          state = :double_quote
          next
        elsif ch == "`"
          buf << ch
          i += 1
          state = :backtick
          next
        elsif ch == ";"
          stmt = buf.strip
          statements << stmt unless statement_empty?(stmt)
          buf = String.new
          i += 1
          next
        else
          buf << ch
          i += 1
          next
        end
      when :line_comment
        buf << ch
        i += 1
        if ch == "\n"
          state = :code
        end
        next
      when :block_comment
        if ch == "*" && nxt == "/"
          buf << ch << nxt
          i += 2
          state = :code
          next
        end
        buf << ch
        i += 1
        next
      when :single_quote
        buf << ch
        i += 1
        if ch == "\\"
          if i < n
            buf << sql[i]
            i += 1
          end
          next
        end
        if ch == "'"
          if i < n && sql[i] == "'"
            buf << sql[i]
            i += 1
            next
          end
          state = :code
        end
        next
      when :double_quote
        buf << ch
        i += 1
        if ch == "\\"
          if i < n
            buf << sql[i]
            i += 1
          end
          next
        end
        if ch == '"'
          if i < n && sql[i] == '"'
            buf << sql[i]
            i += 1
            next
          end
          state = :code
        end
        next
      when :backtick
        buf << ch
        i += 1
        if ch == "\\"
          if i < n
            buf << sql[i]
            i += 1
          end
          next
        end
        if ch == "`"
          if i < n && sql[i] == "`"
            buf << sql[i]
            i += 1
            next
          end
          state = :code
        end
        next
      else
        raise Error, "internal lexer state #{state.inspect}"
      end
    end

    case state
    when :single_quote, :double_quote, :backtick
      raise Error, "unterminated quote (#{state})"
    when :block_comment
      raise Error, "unterminated block comment"
    when :line_comment, :code
      # ok
    else
      raise Error, "internal lexer state #{state.inspect}"
    end

    trailing = buf.strip
    statements << trailing unless statement_empty?(trailing)

    raise Error, "empty or invalid SQL (no statements)" if statements.empty?

    statements
  end

  # True when stripping comments/whitespace leaves no executable SQL.
  def statement_empty?(stmt)
    return true if stmt.nil? || stmt.strip.empty?

    i = 0
    n = stmt.length
    state = :code
    while i < n
      ch = stmt[i]
      nxt = i + 1 < n ? stmt[i + 1] : nil
      case state
      when :code
        if ch == "-" && nxt == "-"
          i += 2
          state = :line_comment
        elsif ch == "/" && nxt == "*"
          i += 2
          state = :block_comment
        elsif ch.match?(/\s/)
          i += 1
        else
          return false
        end
      when :line_comment
        i += 1
        state = :code if ch == "\n"
      when :block_comment
        if ch == "*" && nxt == "/"
          i += 2
          state = :code
        else
          i += 1
        end
      else
        return false
      end
    end
    true
  end
  module_function :statement_empty?

  def write_statement_files(sql, outdir)
    stmts = split(sql)
    raise Error, "outdir required" if outdir.nil? || outdir.to_s.empty?

    require "fileutils"
    FileUtils.mkdir_p(outdir)
    width = [stmts.size.to_s.length, 3].max
    stmts.each_with_index do |stmt, idx|
      name = format("%0#{width}d.sql", idx + 1)
      File.write(File.join(outdir, name), stmt + "\n")
    end
    stmts.size
  end
end
