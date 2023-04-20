
 {#
    This macro returns the description of the key
#}

{% macro get_key_description(key) -%}

    case {{ key }}
        when 0 then 'C'
        when 1 then 'C#'
        when 2 then 'D'
        when 3 then 'D#'
        when 4 then 'E'
        when 5 then 'F'
        when 6 then 'F#'
        when 7 then 'G'
        when 8 then 'G#'
        when 9 then 'A'
        when 10 then 'A#'
        when 11 then 'B'

    end

{%- endmacro %}
