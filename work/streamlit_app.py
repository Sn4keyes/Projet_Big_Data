# streamlit_app.py

import streamlit as st
import pymongo

# Initialisation de la connection à MongoDB

client = pymongo.MongoClient(**st.secrets["mongo"])

# Collecte des données
# st.cache pour s'exécuter toutes les 10 minutes ou quand il y a du changement

@st.cache(ttl=600)
def get_data():
    db = client.mydb
    items = db.mycollection.find()
    items = list(items)  # créé un hashable pour st.cache
    return items

items = get_data()

# Résultats

for item in items:
    st.write(f"{item['name']} has a :{item['pet']}:")