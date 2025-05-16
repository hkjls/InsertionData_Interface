import streamlit as st
import psycopg2
from datetime import datetime, date as dt, time

st.set_page_config(layout="centered")

class sptgd:
    def __init__(self, connexion):
        self.connect = connexion
        
        if "reset" not in st.session_state:
            st.session_state.reset = False
            st.session_state.date = None
            st.session_state.heure = ""
    
    def ui(self, tbname):
        st.title("Formulaire")
        if st.session_state.reset:
            print("rerun")
            print(st.session_state)
            st.session_state.date = None
            st.session_state.heure = ""
            
        with st.form("sptgd"):
            st.subheader("Inserer des données dans la base SPTGD")
            col_date, col_time = st.columns(2)
            with col_date:
                # date = st.date_input("Date", value="2025/01/01")
                # if st.session_state.reset:
                date = st.date_input("Date", value=st.session_state.date)
            with col_time:
                heure = st.text_input("Heure", value=st.session_state.heure, placeholder="hh:mm")
            col1, col2, col3, col4, col5 = st.columns(5)
            with col1:
                security = st.radio("Security", ["oui", "non"])
                sec = "0"
                if security == "oui":
                    sec = "1"
            with col2:
                Tdisp = st.radio("Taux de dispo", ["oui", "non"])
                Td = "0"
                if Tdisp == "oui":
                    Td = "1"
            with col3:
                Preventif = st.radio("Preventif", ["oui", "non"])
                Pr = "0"
                if Preventif == "oui":
                    Pr = "1"
            with col4:
                Gmao = st.radio("Gmao Centralise", ["oui", "non"])
                Gm = "0"
                if Gmao == "oui":
                    Gm = "1"
            with col5:
                Intervention = st.radio("Demande Intervention", ["oui", "non"])
                Inter = "0"
                if Intervention == "oui":
                    Inter = "1"
            
            cola, colb, colc, cold, cole, colf, colg, colh, coli = st.columns(9)
            with colh:
                cancel = st.form_submit_button("Annuler")
            
            with coli:
                submitted = st.form_submit_button("Valider")
            
            
        if submitted:
            st.session_state.reset = False
            connect = self.connect.cursor()
            query = f'INSERT INTO sptgd."{tbname}" ("Date", "Securite", "Taux de dispo", "Preventif", "Gmao centralise", "Demande intervention") VALUES (%s, %s, %s, %s , %s, %s)'
            try:
                date = datetime.combine(date, datetime.strptime(heure, "%H:%M").time())  
                format_date = date.strftime("%Y-%m-%d %H:%M:%S")
                data = [format_date, sec, Td, Pr, Gm, Inter]
                    
                connect.execute(query, data)
            
                self.connect.commit()
            except Exception as e:
                st.text(f"Attention : il y a une erreur au niveau des donnée que vous voulez insérer")
            
        if cancel:
            st.session_state.reset = True
            

if __name__ == "__main__":
    
    connection = psycopg2.connect(
        host="localhost",
        port="5432",
        dbname="postgres",
        user="postgres",
        password="postgres"
        # sslmode="require"
    )
    
    app = sptgd(connection)
    app.ui("sptgd")