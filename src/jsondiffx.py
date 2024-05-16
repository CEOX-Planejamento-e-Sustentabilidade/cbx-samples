import json
#from jsonpatch import apply_patch, JsonPatch
from patchdiff import apply, diff, iapply, to_json

with open('src/json-file/file1.json', 'r') as file:
    dict1 = json.load(file)
with open('src/json-file/file2.json', 'r') as file:
    dict2 = json.load(file)
with open('src/json-file/file3.json', 'r') as file:
    dict3 = json.load(file)
with open('src/json-file/file4.json', 'r') as file:
    dict4 = json.load(file)
with open('src/json-file/file5.json', 'r') as file:
    dict5 = json.load(file)
with open('src/json-file/file6.json', 'r') as file:
    dict6 = json.load(file)

print("--------------------------------------------------------------")

try:
    diff1, diff1_rev = diff(dict1, dict2)
    
    # equal dict2
    # print(json.dumps(apply(dict1, diff1), indent=4))
    # yy = apply(dict1, diff1) == dict2
    # print(yy)
    # # equal dict1
    # print(json.dumps(apply(dict2, diff1_rev), indent=4))
    # yy = apply(dict2, diff1_rev) == dict1
    # print(yy)
    
    diff2, diff2_rev = diff(dict2, dict3)
    diff3, diff3_rev = diff(dict3, dict4)
    diff4, diff4_rev = diff(dict4, dict5)
    diff5, diff5_rev = diff(dict5, dict6)
    
    json5 = apply(dict6, diff5_rev)
    json4 = apply(json5, diff4_rev)
    json3 = apply(json4, diff3_rev)
    json2 = apply(json3, diff2_rev)
    json1 = apply(json2, diff1_rev)
    
    print(json5)
    print(json4)
    print(json5)
    print(json2)
    print(json1)
    
    
    
    print('last json: ', json.dumps(dict6, indent=4))
    print("--------------------------------------------------------------")
    print('version 5: ', json.dumps(apply(dict6, diff5_rev), indent=4))
    print("--------------------------------------------------------------")
    print('version 4: ', json.dumps(apply(dict5, diff4_rev), indent=4))
    print("--------------------------------------------------------------")
    print('version 3: ', json.dumps(apply(dict4, diff3_rev), indent=4))
    print("--------------------------------------------------------------")
    print('version 2: ', json.dumps(apply(dict3, diff2_rev), indent=4))
    print("--------------------------------------------------------------")
    print('original: ', json.dumps(apply(dict2, diff1_rev), indent=4))
    print("--------------------------------------------------------------")
    
    
    
    
    # jsonpatch lib
    # diff1 = JsonPatch.from_diff(dict1, dict2)
    # diff2 = JsonPatch.from_diff(dict2, dict3)
    # diff3 = JsonPatch.from_diff(dict3, dict4)
    # diff4 = JsonPatch.from_diff(dict4, dict5)
    # diff5 = JsonPatch.from_diff(dict5, dict6)
    # print(json.dumps(json.loads(str(diff1)), indent=4))
    # print("--------------------------------------------------------------")
    # print(json.dumps(json.loads(str(diff2)), indent=4))
    # print("--------------------------------------------------------------")
    # print(json.dumps(json.loads(str(diff3)), indent=4))
    # print("--------------------------------------------------------------")
    # print(json.dumps(json.loads(str(diff4)), indent=4))
    # print("--------------------------------------------------------------")
    # print(json.dumps(json.loads(str(diff5)), indent=4))
    # print("--------------------------------------------------------------")
    # print('current json: ', json.dumps(dict6, indent=4))
    # json2 = apply_patch(dict1, diff1)
    # json3 = apply_patch(json2, diff2)
    # json4 = apply_patch(json3, diff3)
    # json5 = apply_patch(json4, diff4)
    # json6 = apply_patch(json5, diff5)    
    # print('json original: ', json.dumps(dict1, indent=4))
    # print('json 2: ', json.dumps(json2, indent=4))
    # print('json 3: ', json.dumps(json3, indent=4))
    # print('json 4: ', json.dumps(json4, indent=4))
    # print('json 5: ', json.dumps(json5, indent=4))
    # print('json 6: ', json.dumps(json6, indent=4))
except Exception as ex:
    print(ex)
