#!/usr/bin/env ruby
# Gabriel Tessarolli

puts "Tipo;Consulta;Run;Tamanho da base;NFragmentos;NResultados Parciais;Tempo Carregando Parciais;Tempo Composicao"
Dir.glob('*.txt') do |log_file|
    if log_file.include? "concat"
        type = "concatenation"
    else
        type = "collection"
    end
    File.open(log_file, "r") do |infile|
        while (line = infile.gets)
            values = line.strip.split(":")
            case values[0].strip
            when "# fragments" 
                nfrags = values[1].strip
            when "Query"
                query = values[1].strip
            when "DB size"
                dbsize = values[1].strip
            when "Run ID"
                run = values[1].strip
            when "# of partial files"
                partials = values[1].strip
            when "Partials loading time"
                timepartials = values[1].strip
            when "Composition time"
                timecomposition = values[1].strip
            end unless values[0].nil?
            
        end
        puts "#{type};#{query};#{run};#{dbsize};#{nfrags};#{partials};#{timepartials};#{timecomposition}"
    end
end

