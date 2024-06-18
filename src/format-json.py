import json

fontes = [
    {"id": 1, "name": "Simcar com CPF", "show": True},
    {"id": 2, "name": "Agrotools FS", "show": True},
    {"id": 3, "name": "Sigef", "show": True},
    {"id": 4, "name": "Manual", "show": True},
    {"id": 5, "name": "Simcar", "show": True}
]

clients = '[{"id_client": 2}, {"id_client": 1}]'
clients_result = json.loads(clients)
id_clients = [str(item["id_client"]) for item in clients_result]
joined_clients = ', '.join(id_clients)
print("clienst: " + joined_clients)

sources = '[{"id_source": 1}, {"id_source": 2}]'
sources_result = json.loads(sources)
id_sources = [str(item["id_source"]) for item in sources_result]
joined_sources = ', '.join(id_sources)
print("sources: "+ joined_sources)

print([fonte["name"] for fonte in fontes if fonte["id"] == 5][0])

print(', '.join([next((f for f in fontes if f["id"] == item["id_source"]), None)["name"] for item in sources_result]))

