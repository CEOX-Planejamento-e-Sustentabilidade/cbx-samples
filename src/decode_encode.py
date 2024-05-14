
import base64
import uuid

transaction_id = str(uuid.uuid4())
context = f"createdby={1};updatedby={1};transactionid={transaction_id}"
encode_context = base64.b64encode(context.encode("ascii"))

print(encode_context)

context = str(encode_context)
if not context is None and context != "":
    context = str(base64.b64decode(context[2:-1]).decode("ascii"))
    ids = context.split(';')
    json = { "createdby": ids[0].split('=')[1], "updatedby": ids[1].split('=')[1], "transactionid": ids[2].split('=')[1] }
    print(json)

