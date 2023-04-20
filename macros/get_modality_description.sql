 {#
    This macro returns the description of the modality
#}

{% macro get_modality_description(mode) -%}

    case {{ mode }}
        when 0 then 'Minor'
        when 1 then 'Major'
       
    end

{%- endmacro %}