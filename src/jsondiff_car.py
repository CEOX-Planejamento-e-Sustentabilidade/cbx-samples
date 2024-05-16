import json
#from jsonpatch import apply_patch, JsonPatch
from patchdiff import apply, diff, iapply, to_json

with open('src/json-file/car1.json', 'r') as file:
    dict1 = json.load(file)
with open('src/json-file/car2.json', 'r') as file:
    dict2 = json.load(file)

print("--------------------------------------------------------------")

try:
    diff1, diff1_rev = diff(dict1, dict2)
    
    json1 = apply(dict2, diff1_rev)
    
    print(to_json(diff1, indent=4))
    print("--------------------------------------------------------------")
    print(to_json(diff1_rev, indent=4))
    print("--------------------------------------------------------------")
    
except Exception as ex:
    print(ex)
