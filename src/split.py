from urllib.parse import quote, unquote

values = ['apple', 'banana%2C%20cherry', 'date']
encoded_values = [quote(value) for value in values]

print(encoded_values)

decoded_values = [unquote(value) for value in values]
print(decoded_values)


def format_like(field, values):    
    values_arr = values.split(';')
    likes = ""
    for val in values_arr:
        if val == "":
            continue

        if likes == "":
            likes += f" 6({field} like %{val}% "
        else:
            likes += f" or {field} like %{val}% "
    return likes + ')' if likes != "" else ""

def format_in(field, values):        
    values_arr = values.split(';')
    count = len(values_arr)
    inn = ""
    for index, val in enumerate(values_arr):
        if val == "":
            continue
        if index == 0:
            inn += f" {field} in ({val}"
        else:
            inn += f",{val}"
    return inn + ')' if inn != "" else ""    

#MTXXXX;MTZZZZ;MTYYYYY
x = format_like('column1', 'MTYYYYY;MTYYYYY;')
print(x)

y = format_in('column2', 'MTXXXX;MTZZZZ;MTYYYYY')
print(y)

