def format_in(field, values_arr):
    inn = ""
    for index, val in enumerate(values_arr):
        if val == "":
            continue
        if index == 0:
            inn += f" {field} in ({val}"
        else:
            inn += f",{val}"
    return inn + ')' if inn != "" else "" 

fontes = ["1", "2", "3"]
#print(f"format_in('(src-id_source)::int', fontes)")
result = f"'{'id_source'}'"
xx = f"and {format_in(f'(src->>{result}::int', fontes)}"
#xx = f" and {format_in('(src->>\'id_source\')::int', fontes)}"

#print(sql)
print(xx)
#sql = f' and {format_in('(src-id_source)::int', fontes)}'

value = 'source'
result = f"'{value}'"
print(result)