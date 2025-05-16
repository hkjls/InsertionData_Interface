# import datetime
import locale
import logging
import os
import time

import psycopg2

import streamlit as st
from pandas._config.config import OptionError

# from dotenv import load_dotenv
from sqlalchemy import create_engine

# load_dotenv()

st.set_page_config(layout="centered")

def time_to_seconds(time_string: str, default_value=86400) -> float:
    """Convert a time string to seconds.

    Args:
        time_string: The time string to convert.

    Returns:
        The time in seconds.
    """
    import datetime

    try:
        time_format = "%H:%M:%S.%f" if "." in str(time_string) else "%H:%M:%S"
        time_object = datetime.datetime.strptime(str(time_string), time_format)
        total_seconds = (
            time_object.hour * 3600
            + time_object.minute * 60
            + time_object.second
            + time_object.microsecond / 1e6
        )
    except ValueError:
        try:
            time_string = str(time_string)
            try:
                days, time_part = time_string.split(" days ")
            except ValueError:
                days, time_part = time_string.split(" day, ")

            hours, minutes, seconds = map(float, time_part.split(":"))
            delta = datetime.timedelta(
                days=int(days), hours=hours, minutes=minutes, seconds=seconds
            )
            total_seconds = delta.total_seconds()
        except Exception as e:
            return default_value
    return total_seconds


def increment_key(s: str):
    try:
        return (
            s.rsplit(sep="_", maxsplit=1)[0]
            + "_"
            + str(int(s.rsplit(sep="_", maxsplit=1)[-1]) + 1)
        )
    except:
        return s


logging.basicConfig(level=logging.INFO)

host = os.getenv("host")
port = os.getenv("port")
dbname = os.getenv("dbname")
user = os.getenv("user")
password = os.getenv("password")
sslmode = os.getenv("sslmode")

from utils_folder.utils import *
from src.utilsAzure import *

try:
    pd.options.mode.copy_on_write = True
except OptionError:
    pass

locale.setlocale(locale.LC_TIME, "")


def to_numeric(x: str):
    if isinstance(x, int):
        x = str(x)
    else:
        x = x.encode("UTF-8")
        x = str(x, encoding="UTF-8")
    x = x.replace(",", ".")
    x = x.replace("\u00a0", "")
    return x


def add_date_data(
    connection,
    engine,
    date: datetime.date,
    data_type: str,
    site: str = "LTH",
    schema: str = "public",
):
    """Add a new date to the Dates_data table.

    Args:
        connection (psycopg2.extensions.connection): The database connection.
        engine (sqlalchemy.engine.base.Engine): The database engine.
        date (datetime.date): The date to add.
        data_type (str): The type of data.
        site (str, optional): The site. Defaults to "LTH".
        schema (str, optional): The schema. Defaults to "public".
    """
    row = {"Site": [site], "Data_type": [data_type], "Date": [date]}
    df_date = pd.DataFrame.from_dict(row)
    df_date.to_sql(
        "Dates_data",
        engine,
        schema=schema,
        if_exists="append",
        index=False,
    )
    connection.commit()


def add_evt_file_callback(OPB_file, date):
    """Upload the events and defaults file to Azure Blob Storage and update the database.

    Args:
        OPB_file (UploadedFile): The uploaded events and defaults file.
        date (datetime.date): The date of the events and defaults data.
        dernier_jour_evt (datetime.date): The last date of events and defaults data.
    """

    upload_Azure_file(
        pd.read_excel(OPB_file),
        f"PFC_LTH/0_raw_data/Extractions_quoti/{date.strftime('%Y%m%d')}/Evenementsetdefauts.xlsx",
    )

    try:
        connection, engine = get_connection()
        update_evts_defauts(connection, engine, date)
        st.success(
            f"Le fichier des évènements et défauts est ajouté dans la base de données."
        )
    except:
        st.error(
            "Le fichier n'est pas en bon format. Veuillez recharger le bon fichier."
        )
    finally:
        connection.close()
        engine.dispose()

    time.sleep(3)
    st.session_state["df_evt_file"] = increment_key(st.session_state["df_evt_file"])
    st.rerun()


def update_injections_antennes(connection, engine, date):
    """Update the injections aux antennes data in the database.

    Args:
        connection (psycopg2.extensions.connection): The database connection.
        engine (sqlalchemy.engine.base.Engine): The database engine.
        date (datetime.date): The date of the injections aux antennes data.
    """

    files = [
        "Injectiondescolisauxantennes_trieur_haut.xlsx",
        "Injectiondescolisauxantennes_trieur_bas.xlsx",
    ]
    for file in files:
        injections_file = get_Azure_file_bytes(
            f"PFC_LTH/0_raw_data/Extractions_quoti/{date.strftime('%Y%m%d')}/{file}"
        )
        if injections_file is not None:

            df_origine = pd.read_excel(
                injections_file,
                usecols=[
                    "Antenne",
                    "Colis codés",
                    "Colis poussés",
                    "Flashage pistolet",
                    "Colis inadmis",
                    "Rejets\nnon lu",
                    "Pourcentage\nRejets non lu",
                    "Multilabels",
                    "Pourcentage Multilabel",
                    "Total injecté",
                    "Temps d'utilisation",
                    "Cadence en fonctionnement",
                    "Date",
                ],
            ).dropna(subset=["Antenne"])
            df_origine.rename(
                columns={
                    "Rejets\nnon lu": "Rejets non lu",
                    "Pourcentage\nRejets non lu": "Pourcentage Rejets non lu",
                },
                inplace=True,
            )

            tuples_to_delete = [
                tuple(x) for x in df_origine[["Date", "Antenne"]].to_numpy()
            ]
            table = "LTH_Injections_Antennes"
            with connection.cursor() as cursor:
                try:
                    chunk_size = 100
                    # Create a list of chunks using a generator expression
                    chunks = [
                        tuples_to_delete[i : i + chunk_size]
                        for i in range(0, len(tuples_to_delete), chunk_size)
                    ]
                    for chunk in chunks:
                        cursor.execute(
                            f"""
                                DELETE FROM public."{table}"
                                WHERE ("Date", "Antenne") IN %s
                            """,
                            (tuple(chunk),),
                        )
                        connection.commit()
                except psycopg2.ProgrammingError:
                    pass

            # Write the DataFrame to the PostgreSQL table
            df_origine.to_sql(
                "LTH_Injections_Antennes",
                engine,
                schema="public",
                if_exists="append",
                index=False,
            )

            connection.commit()


def update_evts_defauts(connection, engine, date):
    """Update the events and defaults data in the database.

    Args:
        connection (psycopg2.extensions.connection): The database connection.
        engine (sqlalchemy.engine.base.Engine): The database engine.
        date (datetime.date): The date of the events and defaults data.
    """

    OPB_file = get_Azure_file_bytes(
        f"PFC_LTH/0_raw_data/Extractions_quoti/{date.strftime('%Y%m%d')}/Evenementsetdefauts.xlsx"
    )
    if OPB_file is not None:
        df_origine = pd.read_excel(
            OPB_file,
            skiprows=5,
            usecols=["Date heure de début", "Date heure de fin", "Machine", "Message"],
        )
        df = df_origine.loc[~df_origine["Message"].str.startswith("Fin :")]
        df["Date"] = date

        table = "LTH_Evt_defauts"
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                    DELETE FROM public."{table}"
                    WHERE "Date"='{date}'
                """
            )
        connection.commit()

        logging.info(f"Updating table {table} with data from {date}")
        df.to_sql(
            table,
            engine,
            schema="public",
            if_exists="append",
            index=False,
        )

        connection.commit()
        upload_opb(OPB_file, connection, engine, date)


def add_evt_file():
    """Add a file uploader for the events and defaults file.
    This function will add a file uploader to the Streamlit app, allowing the user to select an Excel file containing events and defaults data.
    It will also display the last date of events and defaults data and the list of missing dates.
    When the user selects a file and a date, and clicks the "Valider" button, the valider_evts function will be called to process the data.
    """
    if "df_evt_file" not in st.session_state:
        st.session_state["df_evt_file"] = f"df_evt_file_key_0"

    df_evt_file = st.file_uploader(
        "Sélectionner un fichier excel des évènements et défauts",
        type="xlsx",
        key=st.session_state.df_evt_file,
    )
    dernier_jour_evt = get_last_date("OPB")
    st.info(
        f"Dernier jour d'évènements et défauts : {dernier_jour_evt.strftime('%d/%m/%Y')}"
    )
    st.info(
        f"Liste des derniers jours manquant les données OPB : {get_missing_dates('OPB')}"
    )

    if df_evt_file is not None:
        date = st.date_input(
            "Sélectionner la date des données évènements et défauts",
            value=datetime.date.today() - datetime.timedelta(days=1),
            max_value=datetime.date.today() - datetime.timedelta(days=1),
        )

        st.button(
            "Valider",
            key="Valider_evts_btn",
            on_click=add_evt_file_callback,
            args=(df_evt_file, date),
        )


def add_injection_callback(date, excel_file, haut_bas):
    """Upload the injections aux antennes file to Azure Blob Storage and update the database.

    Args:
        date (datetime.date): The date of the injections aux antennes data.
        excel_file (pd.DataFrame): The injections aux antennes data.
        haut_bas (str): The type of trieur (haut or bas).
    """

    excel_file["Date"] = date
    upload_Azure_file(
        excel_file,
        f"PFC_LTH/0_raw_data/Extractions_quoti/{date.strftime('%Y%m%d')}/Injectiondescolisauxantennes_trieur_{haut_bas}.xlsx",
    )
    injection_file_haut = get_Azure_file_bytes(
        f"PFC_LTH/0_raw_data/Extractions_quoti/{date.strftime('%Y%m%d')}/Injectiondescolisauxantennes_trieur_haut.xlsx",
    )
    injection_file_bas = get_Azure_file_bytes(
        f"PFC_LTH/0_raw_data/Extractions_quoti/{date.strftime('%Y%m%d')}/Injectiondescolisauxantennes_trieur_bas.xlsx",
    )

    if injection_file_haut is not None:
        connection, engine = get_connection()
        add_date_data(
            connection=connection,
            engine=engine,
            date=date,
            data_type="Injection_haut",
            site="LTH",
        )
        connection.close()
        engine.dispose()
    if injection_file_bas is not None:
        connection, engine = get_connection()
        add_date_data(
            connection=connection,
            engine=engine,
            date=date,
            data_type="Injection_bas",
            site="LTH",
        )
        connection.close()
        engine.dispose()

    if injection_file_haut is not None and injection_file_bas is not None:
        connection, engine = get_connection()
        df_haut = pd.read_excel(injection_file_haut)
        df_bas = pd.read_excel(injection_file_bas)

        try:
            total_haut = int(
                df_haut.loc[df_haut.Trieur == "Total"]["Total injecté"].iloc[0]
            )
        except:
            st.error(
                "Le format du fichier d'injection du trieur haut n'est pas bon. Merci de recharger le fichier."
            )
            time.sleep(3)
            return
        try:
            total_bas = int(
                df_bas.loc[df_haut.Trieur == "Total"]["Total injecté"].iloc[0]
            )
        except:
            st.error(
                "Le format du fichier d'injection du trieur bas n'est pas bon. Merci de recharger le fichier."
            )
            time.sleep(3)
            return

        total = total_haut + total_bas

        extraction_date = date.strftime("%Y-%m-%d")

        # SQL query to insert a new record into the table
        query = 'INSERT INTO public."Injection_par_jour_LTH" ("Date", "nombre de colis injectés") VALUES (%s, %s);'

        # Execute the query with the values
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                    DELETE FROM public."Injection_par_jour_LTH"
                    WHERE "Date"='{extraction_date}'
                """
            )
            connection.commit()

            cursor.execute(query, (extraction_date, total))
            connection.commit()

        update_injections_antennes(connection, engine, date)
        connection.close()
        engine.dispose()


def add_trafic_sortie_callback(date, excel_file, haut_bas):
    """Upload the trafic par sortie file to Azure Blob Storage and update the database.

    Args:
        date (datetime.date): The date of the trafic par sortie data.
        excel_file (pd.DataFrame): The trafic par sortie data.
        haut_bas (str): The type of trieur (haut or bas).
    """
    excel_file["Date"] = date
    upload_Azure_file(
        excel_file,
        f"PFC_LTH/0_raw_data/Extractions_quoti/{date.strftime('%Y%m%d')}/Trafic_par_sortie_trieur_{haut_bas}.xlsx",
    )
    trafic_sortie_file = get_Azure_file_bytes(
        f"PFC_LTH/0_raw_data/Extractions_quoti/{date.strftime('%Y%m%d')}/Trafic_par_sortie_trieur_{haut_bas}.xlsx",
    )
    if trafic_sortie_file is not None:
        connection, engine = get_connection()

        add_date_data(
            connection=connection,
            engine=engine,
            date=date,
            data_type=f"Trafic_par_sortie_trieur_{haut_bas}",
            site="LTH",
        )

        trafic_sortie_df = pd.read_excel(trafic_sortie_file, skiprows=6)
        # Drop columns with names containing 'Unnamed'
        trafic_sortie_df = trafic_sortie_df.filter(regex="^(?!.*Unnamed)")

        if "Tps Bourrage" in trafic_sortie_df.columns:
            trafic_sortie_df["Tps Bourrage"] = trafic_sortie_df["Tps Bourrage"].apply(
                lambda x: time_to_seconds(x, default_value=0)
            )

        table = "LTH_Trafic_par_sortie"
        trafic_sortie_df["Date"] = date
        trafic_sortie_df = trafic_sortie_df.loc[
            trafic_sortie_df.Trieur == f"Trieur {haut_bas}"
        ]

        cols = [
            "Trieur",
            "Sortie",
            "Nb total de colis",
            "Nb de colis en bac",
            "Type de sortie",
            "Rejet Saturation/CP Absent/Mal positionné",
            "Rejet sortie inhibée/fermée",
            "Nb Saturation",
            "Tps Saturation",
            "Nb Bourrage",
            "Tps Bourrage",
            "Date",
        ]

        if len(trafic_sortie_df) == 0:
            st.error(
                "Le fichier de trafic par sortie n'est pas ajouté dans la base de données. Veuillez vérifier le fichier!"
            )
            time.sleep(3)
            return
        try:
            tuples_to_delete = [
                tuple(x)
                for x in trafic_sortie_df[["Date", "Trieur", "Sortie"]].to_numpy()
            ]
        except KeyError as e:
            raise e
        chunk_size = 100
        # Create a list of chunks using a generator expression
        chunks = [
            tuples_to_delete[i : i + chunk_size]
            for i in range(0, len(tuples_to_delete), chunk_size)
        ]

        cursor = connection.cursor()
        try:
            for chunk in chunks:
                cursor.execute(
                    f"""
                        DELETE FROM public."{table}"
                        WHERE ("Date", "Trieur", "Sortie") IN %s
                        """,
                    (tuple(chunk),),
                )
                connection.commit()
        except:
            pass
        finally:
            cursor.close()

        cols = [
            "Trieur",
            "Sortie",
            "Nb total de colis",
            "Nb de colis en bac",
            "Type de sortie",
            "Rejet Saturation/CP Absent/Mal positionné",
            "Rejet sortie inhibée/fermée",
            "Nb Saturation",
            "Tps Saturation",
            "Nb Bourrage",
            "Tps Bourrage",
            "Date",
        ]
        cols_selected = [col for col in cols if col in trafic_sortie_df.columns]

        trafic_sortie_df = trafic_sortie_df[cols_selected]
        # Write the DataFrame to the PostgreSQL table
        trafic_sortie_df.to_sql(
            table,
            engine,
            schema="public",
            if_exists="append",
            index=False,
        )
        connection.commit()
        connection.close()
        engine.dispose()

    st.success(f"Le fichier de trafic par sortie est ajouté dans la base de données.")
    time.sleep(3)

    st.session_state["trafic_sortie_file"] = increment_key(
        st.session_state["trafic_sortie_file"]
    )
    st.rerun()


def add_inj_file():
    """Add a file uploader for the injections file.
    This function will add a file uploader to the Streamlit app, allowing the user to select an Excel file containing injections data.
    It will also display the last date of injections data and the list of missing dates.
    When the user selects a file and a date, and clicks the "Valider la date saisie..." button, the valider_injection function will be called to process the data.
    """

    if "injection_file" not in st.session_state:
        st.session_state["injection_file"] = "injection_file_key_0"

    df_inj_file = st.file_uploader(
        "Sélectionner un fichier excel des injections",
        key=st.session_state["injection_file"],
        type="xlsx",
    )
    trieur_dict = {"Trieur du haut": "haut", "Trieur du bas": "bas"}
    bas_haut = st.selectbox("Sélectionner le trieur", options=trieur_dict.keys())

    dernier_jour_injection = get_last_date(f"Injection_{trieur_dict[bas_haut]}")

    st.info(
        f"Dernier jour d'injections du trieur {trieur_dict[bas_haut]} : {dernier_jour_injection.strftime('%d/%m/%Y')}",
    )
    if trieur_dict[bas_haut].lower() == "haut":
        st.info(
            f"Liste des derniers jours manquant les données injections du trieur haut : {get_missing_dates('Injection_haut')}"
        )
    else:
        st.info(
            f"Liste des derniers jours manquant les données injections du trieur bas : {get_missing_dates('Injection_bas')}"
        )
    if df_inj_file is not None:
        excel_file = pd.read_excel(df_inj_file)

        date = st.date_input(
            "Date des données d'injection",
            max_value=datetime.date.today() - datetime.timedelta(days=1),
            value=datetime.date.today() - datetime.timedelta(days=1),
        )

        st.button(
            "Valider la date saisie...",
            key="valider_inj_file_btn",
            on_click=add_injection_callback,
            args=(date, excel_file, trieur_dict[bas_haut]),
        )


def add_trafic_sortie_file():
    """Add a file uploader for the trafic par sortie file.
    This function will add a file uploader to the Streamlit app, allowing the user to select an Excel file containing trafic par sortie data.
    It will also display the last date of trafic par sortie data and the list of missing dates.
    When the user selects a file and a date, and clicks the "Valider la date saisie..." button, the valider_trafic_sortie function will be called to process the data.
    """

    if "trafic_sortie_file" not in st.session_state:
        st.session_state["trafic_sortie_file"] = "trafic_sortie_file_key_0"

    df_trafic_sortie_file = st.file_uploader(
        "Sélectionner un fichier excel des trafics par sortie",
        key=st.session_state["trafic_sortie_file"],
        type="xlsx",
    )
    trieur_dict = {"Trieur du haut": "haut", "Trieur du bas": "bas"}
    bas_haut = st.selectbox(
        "Sélectionner le trieur", options=trieur_dict.keys(), key="trieur_trafic_sortie"
    )

    dernier_jour_trafic_sortie = get_last_date(
        f"Trafic_par_sortie_trieur_{trieur_dict[bas_haut]}"
    )
    st.info(
        f"Dernier jour de trafic par sortie du trieur {trieur_dict[bas_haut]} : {dernier_jour_trafic_sortie.strftime('%d/%m/%Y')}",
    )

    if trieur_dict[bas_haut].lower() == "haut":
        st.info(
            f"Liste des derniers jours manquant les données trafic par sortie du trieur haut : {get_missing_dates('Trafic_par_sortie_trieur_haut')}"
        )
    else:
        st.info(
            f"Liste des derniers jours manquant les données trafic par sortie du trieur bas : {get_missing_dates('Trafic_par_sortie_trieur_bas')}"
        )
    if df_trafic_sortie_file is not None:
        excel_file = pd.read_excel(df_trafic_sortie_file)

        date = st.date_input(
            "Date des données de trafic par sortie",
            max_value=datetime.date.today() - datetime.timedelta(days=1),
            value=datetime.date.today() - datetime.timedelta(days=1),
        )

        st.button(
            "Valider la date saisie...",
            key="valider_trafic_sortie_file_btn",
            on_click=add_trafic_sortie_callback,
            args=(date, excel_file, trieur_dict[bas_haut]),
        )


def add_prod_callback(date, excel_file):
    """Upload the production file to Azure Blob Storage and update the database.

    Args:
        date (datetime.date): The date of the production data.
        excel_file (pd.DataFrame): The production data.
    """

    excel_file["Date"] = date
    upload_Azure_file(
        excel_file,
        f"PFC_LTH/0_raw_data/Extractions_quoti/{date.strftime('%Y%m%d')}/Temps_de_fonctionnement_et_arrêts_machine.xlsx",
    )

    try:
        connection, engine = get_connection()

        update_temps_fonctionnement(connection, engine, date)

        st.success(
            f"Le fichier des temps de fonctionnement et arrêts machine est ajouté dans la base de données."
        )
        st.session_state["fonctionnement_file"] = increment_key(
            st.session_state["fonctionnement_file"]
        )
    except:
        st.error(
            "Le fichier n'est pas en bon format. Veuillez recharger le bon fichier."
        )
    finally:
        connection.close()
        engine.dispose()

    time.sleep(3)
    st.rerun()


def add_prod_file():
    """Add a file uploader for the production file.
    This function will add a file uploader to the Streamlit app, allowing the user to select an Excel file containing production data.
    It will also display the last date of production data and the list of missing dates.
    When the user selects a file and a date, and clicks the "Valider la date saisie..." button, the valider_prod function will be called to process the data.
    """

    if "fonctionnement_file" not in st.session_state:
        st.session_state["fonctionnement_file"] = f"fonctionnement_file_key_0"
    df_prod_file = st.file_uploader(
        "Sélectionner un fichier excel du temps de fonctionnement et arrêts machine",
        key=st.session_state["fonctionnement_file"],
        type="xlsx",
    )

    dernier_jour_prod = get_last_date("Temps_fonctionnement")

    st.info(
        f"Dernier jour du temps de fonctionnement et arrêts machine : {dernier_jour_prod.strftime('%d/%m/%Y')}",
    )
    st.info(
        f"Liste des derniers jours manquant les données de fonctionnement et arrêts machine : {get_missing_dates('Temps_fonctionnement')}"
    )

    if df_prod_file is not None:
        excel_file = pd.read_excel(df_prod_file)

        date = st.date_input(
            "Date des données du temps de fonctionnement et arrêts machine",
            max_value=datetime.date.today() - datetime.timedelta(days=1),
            value=datetime.date.today() - datetime.timedelta(days=1),
        )

        st.button(
            "Valider la date saisie...",
            key="valider_prod_btn",
            on_click=add_prod_callback,
            args=(date, excel_file),
        )


def add_qualite_callback(date, excel_file):
    """Upload the quality file to Azure Blob Storage and update the database.

    Args:
        date (datetime.date): The date of the quality data.
        excel_file (pd.DataFrame): The quality data.
    """
    excel_file["Date"] = date

    upload_Azure_file(
        excel_file,
        f"PFC_LTH/0_raw_data/Extractions_quoti/{date.strftime('%Y%m%d')}/Qualité_de_tri.xlsx",
    )
    connection, engine = get_connection()

    try:
        update_qualite_tri_data(connection, engine, date)

        st.success(f"Le fichier de qualité de tri est ajouté dans la base de données.")
        time.sleep(3)
        st.session_state["df_qualite_file"] = increment_key(
            st.session_state["df_qualite_file"]
        )
    except:
        st.error(
            "Le fichier n'est pas en bon format. Veuillez recharger le bon fichier. "
        )
        time.sleep(3)
    finally:
        connection.close()
        engine.dispose()

    st.rerun()


def update_trafic_sortie_data(connection, date, file):
    trafic_sortie_df = pd.read_excel(file, skiprows=6)
    # Drop columns with names containing 'Unnamed'
    trafic_sortie_df = trafic_sortie_df.filter(regex="^(?!.*Unnamed)")

    trafic_sortie_df["Tps Bourrage"] = trafic_sortie_df["Tps Bourrage"].apply(
        lambda x: time_to_seconds(x, default_value=0)
    )

    table = "LTH_Trafic_par_sortie"
    trafic_sortie_df["Date"] = date

    try:
        tuples_to_delete = [
            tuple(x) for x in trafic_sortie_df[["Date", "Trieur", "Sortie"]].to_numpy()
        ]
    except KeyError as e:
        raise e
    chunk_size = 100
    # Create a list of chunks using a generator expression
    chunks = [
        tuples_to_delete[i : i + chunk_size]
        for i in range(0, len(tuples_to_delete), chunk_size)
    ]

    cursor = connection.cursor()
    try:
        for chunk in chunks:
            cursor.execute(
                f"""
                DELETE FROM public."{table}"
                WHERE ("Date", "Trieur", "Sortie") IN %s
                """,
                (tuple(chunk),),
            )
            connection.commit()
    except:
        pass
    finally:
        cursor.close()

    # Create an SQLAlchemy engine to connect to the PostgreSQL database
    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    )

    # Write the DataFrame to the PostgreSQL table
    trafic_sortie_df.to_sql(
        table,
        engine,
        schema="public",
        if_exists="append",
        index=False,
    )
    connection.commit()

    add_date_data(
        connection=connection,
        engine=engine,
        date=date,
        data_type="Trafic_par_sortie",
        site="LTH",
    )


def update_qualite_tri_data(connection, engine, date):
    """Update the quality of sorting data in the database.

    Args:
        connection (psycopg2.extensions.connection): The database connection.
        engine (sqlalchemy.engine.base.Engine): The database engine.
        date (datetime.date): The date of the quality of sorting data.
    """

    qualite_tri_file = get_Azure_file_bytes(
        f"PFC_LTH/0_raw_data/Extractions_quoti/{date.strftime('%Y%m%d')}/Qualité_de_tri.xlsx"
    )
    if qualite_tri_file is not None:

        qualite_tri_df = pd.read_excel(qualite_tri_file, skiprows=3)
        # Drop columns with names containing 'Unnamed'
        qualite_tri_df = qualite_tri_df.filter(regex="^(?!.*Unnamed)")
        for column in ["Trieur", "Tri/contrôle ou rejet", "Type de tri/contrôle/rejet"]:
            qualite_tri_df[column] = qualite_tri_df[column].ffill()
        qualite_tri_df.dropna(subset=["Détail de tri/rejet"], inplace=True)

        table = "LTH_Qualite_de_tri"
        qualite_tri_df["Date"] = date

        columns = [
            "Trieur",
            "Tri/contrôle ou rejet",
            "Type de tri/contrôle/rejet",
            "Détail de tri/rejet",
            "Nb total colis",
            "Nb de colis en bac",
            "En pourcentage",
            "Date",
        ]
        qualite_tri_df = qualite_tri_df[columns]
        try:
            tuples_to_delete = [
                tuple(x)
                for x in qualite_tri_df[
                    [
                        "Date",
                        "Trieur",
                        "Tri/contrôle ou rejet",
                        "Type de tri/contrôle/rejet",
                        "Détail de tri/rejet",
                    ]
                ].to_numpy()
            ]
        except KeyError as e:
            raise e
        chunk_size = 100
        # Create a list of chunks using a generator expression
        chunks = [
            tuples_to_delete[i : i + chunk_size]
            for i in range(0, len(tuples_to_delete), chunk_size)
        ]

        cursor = connection.cursor()
        try:
            for chunk in chunks:
                cursor.execute(
                    """
                    DELETE FROM public."LTH_Qualite_de_tri"
                    WHERE ("Date", "Trieur", "Tri/contrôle ou rejet", "Type de tri/contrôle/rejet", "Détail de tri/rejet") IN %s
                    """,
                    (tuple(chunk),),
                )
                connection.commit()
        except:
            pass
        finally:
            cursor.close()

        # Write the DataFrame to the PostgreSQL table
        qualite_tri_df.to_sql(
            table,
            engine,
            schema="public",
            if_exists="append",
            index=False,
        )
        connection.commit()

        add_date_data(
            connection=connection,
            engine=engine,
            date=date,
            data_type="Qualité_de_tri",
            site="LTH",
        )


def remove_from_first_empty_row(df: pd.DataFrame) -> pd.DataFrame:
    """Supprimer toutes les lignes d'un Dataframe après la première ligne vide

    Args:
        df (pd.DataFrame)

    Returns:
        pd.DataFrame: résultat
    """

    # Find the index of the first empty row
    first_empty_row_index = df.index[df.isnull().all(axis=1)].min()
    # If there is no empty row, set it to the last row + 1
    if pd.isna(first_empty_row_index):
        first_empty_row_index = len(df)
    # Create a new DataFrame with rows up to the first empty row
    new_df = df.iloc[:first_empty_row_index]
    return new_df


def update_temps_fonctionnement(connection, engine, date):
    """Update the temps de fonctionnement data in the database.

    Args:
        connection (psycopg2.extensions.connection): The database connection.
        engine (sqlalchemy.engine.base.Engine): The database engine.
        date (datetime.date): The date of the temps de fonctionnement data.
    """

    tmp_fonctionnement_file = get_Azure_file_bytes(
        f"PFC_LTH/0_raw_data/Extractions_quoti/{date.strftime('%Y%m%d')}/Temps_de_fonctionnement_et_arrêts_machine.xlsx"
    )
    if tmp_fonctionnement_file is not None:
        tmp_fonctionnement_arret_df = pd.read_excel(tmp_fonctionnement_file, skiprows=3)
        tmp_fonctionnement_arret_df = tmp_fonctionnement_arret_df.filter(
            regex="^(?!.*Unnamed)"
        )
        tmp_fonctionnement_arret_df.columns = [
            "Système",
            "Temps de fonctionnement (s)",
            "Date",
        ]
        tmp_fonctionnement_arret_df.drop(columns=["Date"], inplace=True)
        tmp_fonctionnement_arret_df = remove_from_first_empty_row(
            tmp_fonctionnement_arret_df
        )
        # Drop columns with names containing 'Unnamed'

        tmp_fonctionnement_arret_df = tmp_fonctionnement_arret_df.loc[
            tmp_fonctionnement_arret_df["Système"] != "Total"
        ]
        tmp_fonctionnement_arret_df["Temps de fonctionnement (s)"] = (
            tmp_fonctionnement_arret_df["Temps de fonctionnement (s)"].apply(
                lambda x: time_to_seconds(x)
            )
        )

        table = "LTH_Tmps_fonctionnement"
        tmp_fonctionnement_arret_df["Date"] = date

        try:
            tuples_to_delete = [
                tuple(x)
                for x in tmp_fonctionnement_arret_df[["Date", "Système"]].to_numpy()
            ]
        except KeyError as e:
            raise e
        chunk_size = 100
        # Create a list of chunks using a generator expression
        chunks = [
            tuples_to_delete[i : i + chunk_size]
            for i in range(0, len(tuples_to_delete), chunk_size)
        ]

        cursor = connection.cursor()
        try:
            for chunk in chunks:
                cursor.execute(
                    """
                    DELETE FROM public."LTH_Tmps_fonctionnement"
                    WHERE ("Date", "Système") IN %s
                    """,
                    (tuple(chunk),),
                )
                connection.commit()
        except:
            pass
        finally:
            cursor.close()

        # Write the DataFrame to the PostgreSQL table
        tmp_fonctionnement_arret_df.to_sql(
            table,
            engine,
            schema="public",
            if_exists="append",
            index=False,
        )
        connection.commit()

        add_date_data(
            connection=connection,
            engine=engine,
            date=date,
            data_type="Temps_fonctionnement",
            site="LTH",
        )


def add_qualite_file():
    """Add a file uploader for the quality file.
    This function will add a file uploader to the Streamlit app, allowing the user to select an Excel file containing quality data.
    It will also display the last date of quality data and the list of missing dates.
    When the user selects a file and a date, and clicks the "Valider la date saisie..." button, the valider_qualite function will be called to process the data.
    """

    if "df_qualite_file" not in st.session_state:
        st.session_state["df_qualite_file"] = f"df_qualite_file_key_0"
    df_qualite_file = st.file_uploader(
        "Sélectionner un fichier excel de qualité de tri",
        key=st.session_state["df_qualite_file"],
        type="xlsx",
    )

    dernier_jour_qualite = get_last_date("Qualité_de_tri")

    st.info(
        f"Dernier jour de qualité de tri : {dernier_jour_qualite.strftime('%d/%m/%Y')}",
    )
    st.info(
        f"Liste des derniers jours manquant les données Qualité de tri : {get_missing_dates('Qualité_de_tri')}"
    )

    if df_qualite_file is not None:
        excel_file = pd.read_excel(df_qualite_file)
        date = st.date_input(
            "Date des données de qualité de tri",
            max_value=datetime.date.today() - datetime.timedelta(days=1),
            value=datetime.date.today() - datetime.timedelta(days=1),
        )
        st.button(
            "Valider la date saisie...",
            key="valider_qualite_btn",
            on_click=add_qualite_callback,
            args=(date, excel_file),
        )


# def add_trafic_sortie():
#     if "df_trafic_sortie_file" not in st.session_state:
#         st.session_state["df_trafic_sortie_file"] = f"df_trafic_sortie_file_key_0"
#
#     df_trafic_sortie_file = st.file_uploader(
#         "Sélectionner un fichier excel de trafic par sortie",
#         key=st.session_state["df_trafic_sortie_file"],
#         type="xlsx",
#     )
#
#     dernier_jour_trafic = get_last_date("Trafic_par_sortie")
#     st.info(
#         f"Dernier jour de trafic par sortie : {dernier_jour_trafic.strftime('%d/%m/%Y')}",
#     )
#     st.info(
#         f"Liste des derniers jours manquant les données Trafic par sortie : {get_missing_dates('Trafic_par_sortie')}"
#     )
#
#     if df_trafic_sortie_file is not None:
#         excel_file = pd.read_excel(df_trafic_sortie_file)
#         date = st.date_input(
#             "Date des données de Trafic par sortie",
#             max_value=datetime.date.today() - datetime.timedelta(days=1),
#             value=datetime.date.today() - datetime.timedelta(days=1),
#         )
#         st.button(
#             "Valider la date saisie...",
#             key="valider_trafic_btn",
#             on_click=valider_trafic_sortie,
#             args=(date, excel_file, dernier_jour_trafic),
#         )


# def upload_interventions_callback(
#     interventions_file, connection, engine, extraction_date
# ):
#     """Upload the interventions file to the database.
#
#     Args:
#         interventions_file (UploadedFile): The uploaded interventions file.
#         connection (psycopg2.extensions.connection): The database connection.
#         engine (sqlalchemy.engine.base.Engine): The database engine.
#         extraction_date (datetime.date): The date of extraction of the interventions data.
#     """
#
#     df = pd.read_excel(interventions_file)
#
#     for date_col in [
#         "Date/heure de fin de l'intervention",
#         "Date initiale de début",
#         "Date/heure de début de l'intervention",
#         "Date de dernière modification",
#     ]:
#         df[date_col] = pd.to_datetime(df[date_col], dayfirst=True)
#     df["Charge prévue"] = df["Charge prévue"].apply(lambda x: time_to_seconds(x))
#
#     tuples_to_delete = df["Code de l'intervention"].to_list()
#     chunk_size = 100
#     # Create a list of chunks using a generator expression
#     chunks = [
#         tuples_to_delete[i : i + chunk_size]
#         for i in range(0, len(tuples_to_delete), chunk_size)
#     ]
#
#     cursor = connection.cursor()
#     for chunk in chunks:
#         query = """
#             DELETE FROM public."Interventions_LTH"
#             WHERE ("Code de l'intervention") IN %s
#             """
#         # print(cursor.mogrify(query, (tuple(chunk),)))
#         cursor.execute(query, (tuple(chunk),))
#         connection.commit()
#     cursor.close()
#
#     # Write the DataFrame to the PostgreSQL table
#     df.to_sql(
#         "Interventions_LTH",
#         engine,
#         schema="public",
#         if_exists="append",
#         index=False,
#     )
#
#     connection.commit()
#
#     add_date_data(
#         connection=connection,
#         engine=engine,
#         date=extraction_date,
#         data_type="Interventions",
#         site="LTH",
#     )


# Remove connection and engine from arguments
def upload_interventions_callback(interventions_file, extraction_date):
    """Upload the interventions file to the database.

    Args:
        interventions_file (UploadedFile): The uploaded interventions file.
        extraction_date (datetime.date): The date of extraction of the interventions data.
    """
    connection = None  # Initialize to None
    engine = None  # Initialize to None
    try:
        # Establish connection INSIDE the callback
        connection, engine = get_connection()

        df = pd.read_excel(interventions_file)

        for date_col in [
            "Date/heure de fin de l'intervention",
            "Date initiale de début",
            "Date/heure de début de l'intervention",
            "Date de dernière modification",
        ]:
            df[date_col] = pd.to_datetime(df[date_col], dayfirst=True)
        df["Charge prévue"] = df["Charge prévue"].apply(lambda x: time_to_seconds(x))

        tuples_to_delete = df["Code de l'intervention"].to_list()
        chunk_size = 100
        # Create a list of chunks using a generator expression
        chunks = [
            tuples_to_delete[i : i + chunk_size]
            for i in range(0, len(tuples_to_delete), chunk_size)
        ]

        cursor = connection.cursor()
        try:  # Add a try-finally for the cursor as well
            for chunk in chunks:
                query = """
                    DELETE FROM public."Interventions_LTH"
                    WHERE ("Code de l'intervention") IN %s
                    """
                # print(cursor.mogrify(query, (tuple(chunk),)))
                cursor.execute(query, (tuple(chunk),))
                connection.commit()
        finally:
            cursor.close()  # Ensure cursor is closed

        # Write the DataFrame to the PostgreSQL table
        df.to_sql(
            "Interventions_LTH",
            engine,
            schema="public",
            if_exists="append",
            index=False,
        )

        connection.commit()

        add_date_data(
            connection=connection,
            engine=engine,
            date=extraction_date,
            data_type="Interventions",
            site="LTH",
        )
        # Success message can go here if needed, before closing connection
        st.success(
            "Le fichier des interventions a été chargé avec succès."
        )  # Example success message

    except Exception as e:
        # Log the error or show an error message to the user
        logging.error(f"Error during intervention upload: {e}")
        st.error(f"Une erreur est survenue lors du chargement des interventions: {e}")
    finally:
        # Ensure connection and engine are closed/disposed even if errors occur
        if connection:
            connection.close()
        if engine:
            engine.dispose()


def upload_interventions():
    """Add a file uploader for the interventions file.
    This function will add a file uploader to the Streamlit app, allowing the user to select an Excel file containing interventions data.
    It will also display the last date of interventions data.
    When the user selects a file and a date, and clicks the "Valider le chargement des interventions LTH" button, the upload_interventions_callback function will be called to process the data.
    """

    if "interventions_file" not in st.session_state:
        st.session_state["interventions_file"] = f"interventions_file_key_0"
    interventions_file = st.file_uploader(
        "Sélectionner un fichier Excel des interventions",
        key=st.session_state["interventions_file"],
    )

    # Establish a connection to the PostgreSQL database - ONLY for getting last date
    # This connection is short-lived and closed by get_last_date()
    last_extraction_date = get_last_date("Interventions")
    st.info(
        f"Date de la dernière extraction des interventions: {last_extraction_date.strftime('%d/%m/%Y')}"
    )

    if interventions_file is not None:

        extraction_date = st.date_input(
            "Sélectionner la date de l'extraction",
            value=datetime.date.today(),
            max_value=datetime.date.today(),
            key="extraction_date_5",
        )

        if (
            last_extraction_date is None
            or last_extraction_date
            == 0  # Consider if 0 is a valid comparison here, maybe just check for None?
            or extraction_date >= last_extraction_date
        ):
            if st.button(
                "Valider le chargement des interventions LTH",
                on_click=upload_interventions_callback,
                args=(interventions_file, extraction_date),
            ):
                # This part might not be needed anymore if success/rerun is handled in callback
                st.session_state["interventions_file"] = increment_key(
                    st.session_state["interventions_file"]
                )
                # Consider adding st.rerun() here or in the callback after success message
                st.rerun()  # Rerun after button click initiates the callback
        else:
            st.info(
                "Les données plus rcentes ont été déjà chargées dans la base de données."
            )


# def upload_interventions():
#     """Add a file uploader for the interventions file.
#     This function will add a file uploader to the Streamlit app, allowing the user to select an Excel file containing interventions data.
#     It will also display the last date of interventions data.
#     When the user selects a file and a date, and clicks the "Valider le chargement des interventions LTH" button, the upload_interventions_callback function will be called to process the data.
#     """
#
#     if "interventions_file" not in st.session_state:
#         st.session_state["interventions_file"] = f"interventions_file_key_0"
#     interventions_file = st.file_uploader(
#         "Sélectionner un fichier Excel des interventions",
#         key=st.session_state["interventions_file"],
#     )
#
#     # Establish a connection to the PostgreSQL database
#
#     last_extraction_date = get_last_date("Interventions")
#     st.info(
#         f"Date de la dernière extraction des interventions: {last_extraction_date.strftime('%d/%m/%Y')}"
#     )
#
#     if interventions_file is not None:
#         connection, engine = get_connection()
#         extraction_date = st.date_input(
#             "Sélectionner la date de l'extraction",
#             value=datetime.date.today(),
#             max_value=datetime.date.today(),
#             key="extraction_date_5",
#         )
#
#         if (
#             last_extraction_date is None
#             or last_extraction_date == 0
#             or extraction_date >= last_extraction_date
#         ):
#             if st.button(
#                 "Valider le chargement des interventions LTH",
#                 on_click=upload_interventions_callback,
#                 args=(interventions_file, connection, engine, extraction_date),
#             ):
#                 st.session_state["interventions_file"] = increment_key(
#                     st.session_state["interventions_file"]
#                 )
#         else:
#             st.info(
#                 "Les données plus récentes ont été déjà chargées dans la base de données."
#             )
#         connection.close()
#         engine.dispose()


def get_connection():
    """Etablir la connection avec la base de données Postgresql

    Returns:
        connection, engine
    """
    # try:
    #     import dotenv
    #
    #     dotenv.load_dotenv()
    # except:
    #     pass
    host = os.getenv("host")
    port = os.getenv("port")
    dbname = os.getenv("dbname")
    user = os.getenv("user")
    password = os.getenv("password")
    sslmode = os.getenv("sslmode")
    # Create an SQLAlchemy engine to connect to the PostgreSQL database
    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    )
    # Establish a connection to the PostgreSQL database
    connection = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
        sslmode=sslmode,
    )
    return connection, engine


def get_last_date(data_type: str) -> datetime.date:
    """Get the last date of extraction for a given data type.

    Args:
        data_type (str): The type of data.

    Returns:
        datetime.date: The last date of extraction.
    """

    connection, _ = get_connection()
    try:
        query = f'SELECT "Date" FROM public."Dates_data" WHERE "Site" = \'LTH\' AND "Data_type" = %s ORDER BY "Date" DESC LIMIT 1;'
        with connection.cursor() as cursor:
            # Execute the query with the filter values
            cursor.execute(query, (data_type,))
            # Fetch the result
            last_extraction_date = cursor.fetchone()
            if len(last_extraction_date) >= 0:
                last_extraction_date = last_extraction_date[0]
            else:
                if data_type == "Etat_stock":
                    last_extraction_date = datetime.date(2025, 3, 31)
                elif data_type == "OPB":
                    last_extraction_date = datetime.date(2025, 4, 1)
                elif "Injection" in data_type:
                    last_extraction_date = datetime.date(2025, 4, 1)
                elif data_type == "Qualité de tri":
                    last_extraction_date = datetime.date(2025, 4, 1)
                elif data_type == "Temps_fonctionnement":
                    last_extraction_date = datetime.date(2025, 4, 1)
                elif "Trafic_par_sortie_trieur_" in data_type:
                    last_extraction_date = datetime.date(2025, 4, 1)
                elif data_type == "Interventions":
                    last_extraction_date = datetime.date(2025, 3, 30)
                elif data_type == "Mvt_stock":
                    last_extraction_date = datetime.date(2025, 3, 30)
    except Exception as e:
        if data_type == "Etat_stock":
            last_extraction_date = datetime.date(2025, 3, 31)
        elif data_type == "OPB":
            last_extraction_date = datetime.date(2025, 4, 1)
        elif "Injection" in data_type:
            last_extraction_date = datetime.date(2025, 4, 1)
        elif data_type == "Qualité de tri":
            last_extraction_date = datetime.date(2025, 4, 1)
        elif data_type == "Temps_fonctionnement":
            last_extraction_date = datetime.date(2025, 4, 1)
        elif "Trafic_par_sortie_trieur_" in data_type:
            last_extraction_date = datetime.date(2025, 4, 1)
        elif data_type == "Interventions":
            last_extraction_date = datetime.date(2025, 3, 30)
        elif data_type == "Mvt_stock":
            last_extraction_date = datetime.date(2025, 3, 30)
    finally:
        connection.close()
    return last_extraction_date


def upload_mvt_stock_callback(mvt_stock_file, extraction_date):
    """Upload the movements of stock file to the database.

    Args:
        mvt_stock_file (UploadedFile): The uploaded movements of stock file.
        extraction_date (datetime.date): The date of extraction of the movements of stock data.
    """

    df = pd.read_excel(mvt_stock_file)
    date_columns = [
        "Date et heure du mouvement de stock",
        "Date et heure de valorisation stock",
    ]
    for col in date_columns:
        df[col] = pd.to_datetime(df[col], dayfirst=True)

    connection, engine = get_connection()

    logging.info(
        f"Uploading mvt_stock_file to database with extraction date: {extraction_date}"
    )

    df.to_sql(
        "LTH_MVT_Stock",
        engine,
        schema="public",
        if_exists="append",
        index=False,
    )
    connection.commit()

    logging.info(
        """Removing duplicates from LTH_MVT_Stock table based on 'Date et heure du mouvement de stock', 
                 'Article', 'Quantité du mouvement', and 'Magasin de stockage' columns"""
    )
    with connection.cursor() as cursor:
        cursor.execute(
            """
            DELETE FROM public."LTH_MVT_Stock" a
            WHERE a.ctid <> (
                SELECT max(b.ctid)
                FROM public."LTH_MVT_Stock" b
                WHERE a."Date et heure du mouvement de stock" = b."Date et heure du mouvement de stock"
                AND a."Article" = b."Article"
                AND a."Quantité du mouvement" = b."Quantité du mouvement"
                AND a."Magasin de stockage" = b."Magasin de stockage"
            );
        """
        )
        connection.commit()

    add_date_data(
        connection=connection,
        engine=engine,
        date=extraction_date,
        data_type="Mvt_stock",
        site="LTH",
    )

    st.success(
        f"Le fichier des mouvements de stock est ajouté dans la base de données."
    )
    time.sleep(3)

    # Close the connection
    connection.close()
    engine.dispose()

    st.rerun()


def upload_mvt_stock():
    """Add a file uploader for the movements of stock file.
    This function will add a file uploader to the Streamlit app, allowing the user to select an Excel file containing movements of stock data.
    It will also display the last date of movements of stock data.
    When the user selects a file and a date, and clicks the "Valider le chargement des mouvements de stock LTH" button, the upload_mvt_stock_callback function will be called to process the data.
    """

    if "mvt_file" not in st.session_state:
        st.session_state["mvt_file"] = f"mvt_file_key_0"
    mvt_stock_file = st.file_uploader(
        "Sélectionner un fichier Excel des mouvements de stock",
        key=st.session_state["mvt_file"],
    )
    last_extraction_date = get_last_date(data_type="Mvt_stock")

    st.info(
        f"Date de la dernière extraction des mouvements de stock : {last_extraction_date.strftime('%d/%m/%Y')}"
    )

    if mvt_stock_file is not None:
        extraction_date = st.date_input(
            "Sélectionner la date de l'extraction",
            value=datetime.date.today(),
            max_value=datetime.date.today(),
            key="date_input_2",
        )

        if (
            last_extraction_date is None
            or last_extraction_date == 0
            or extraction_date >= last_extraction_date
        ):
            if st.button(
                "Valider le chargement des mouvements de stock LTH",
                on_click=upload_mvt_stock_callback,
                args=(mvt_stock_file, extraction_date),
            ):
                st.session_state["mvt_file"] = increment_key(
                    st.session_state["mvt_file"]
                )
        else:
            st.info(
                "Les données plus récentes ont été déjà chargées dans la base de données."
            )


def upload_stock_callback(etat_stock, extraction_date):
    """Upload the stock (inventaire) file to the database.

    Args:
        etat_stock (UploadedFile): The uploaded stock file.
        extraction_date (datetime.date): The date of extraction of the stock data.
    """

    logging.info(
        f"Uploading etat_stock to database with extraction date: {extraction_date}"
    )
    df = pd.read_excel(etat_stock)
    connection, engine = get_connection()
    df.to_sql(
        "LTH_Inventaire",
        engine,
        schema="public",
        if_exists="append",
        index=False,
    )

    connection.commit()

    logging.info(
        """Removing duplicates from LTH_Inventaire table based on 'Article', 'Magasin de stockage' columns"""
    )
    with connection.cursor() as cursor:
        cursor.execute(
            """
            DELETE FROM public."LTH_Inventaire" a
            WHERE a.ctid <> (
                SELECT max(b.ctid)
                FROM public."LTH_Inventaire" b
                WHERE a."Article" = b."Article"
                AND a."Magasin de stockage" = b."Magasin de stockage"
            );
        """
        )
        connection.commit()

    add_date_data(
        connection=connection,
        engine=engine,
        date=extraction_date,
        data_type="Etat_stock",
        site="LTH",
    )

    st.success(f"Le fichier d'inventaire est ajouté dans la base de données.")
    time.sleep(3)

    connection.close()
    engine.dispose()

    st.rerun()


def upload_inventaire():
    """Add a file uploader for the stock (inventaire) file.
    This function will add a file uploader to the Streamlit app, allowing the user to select an Excel file containing stock data.
    It will also display the last date of stock data.
    When the user selects a file and a date, and clicks the "Valider le chargement de l'inventaire LTH" button, the upload_stock_callback function will be called to process the data.
    """

    if "inv_file" not in st.session_state:
        st.session_state["inv_file"] = f"inv_file_key_0"
    etat_stock = st.file_uploader(
        "Sélectionner un fichier Excel de l'inventaire",
        key=st.session_state["inv_file"],
    )

    last_extraction_date = get_last_date(data_type="Etat_stock")

    st.info(
        f"Date de la dernière extraction de l'inventaire: {last_extraction_date.strftime('%d/%m/%Y')}"
    )

    if etat_stock is not None:
        extraction_date = st.date_input(
            "Sélectionner la date de l'extraction",
            value=datetime.date.today(),
            max_value=datetime.date.today(),
            key="date_input_3",
        )

        if (
            last_extraction_date is None
            or last_extraction_date == 0
            or extraction_date >= last_extraction_date
        ):
            if st.button(
                "Valider le chargement de l'inventaire LTH",
                on_click=upload_stock_callback,
                args=(etat_stock, extraction_date),
            ):
                st.session_state["inv_file"] = increment_key(
                    st.session_state["inv_file"]
                )
        else:
            st.info(
                "Les données plus récentes ont été déjà chargées dans la base de données."
            )


def upload_poids_carbone_callback(poids_carbone, connection, engine, extraction_date):
    """Upload the poids carbone file to the database.

    Args:
        poids_carbone (UploadedFile): The uploaded poids carbone file.
        connection (psycopg2.extensions.connection): The database connection.
        engine (sqlalchemy.engine.base.Engine): The database engine.
        extraction_date (datetime.date): The date of extraction of the poids carbone data.
    """

    logging.info(
        f"Uploading poids_carbone to database with extraction date: {extraction_date}"
    )

    df = pd.read_excel(poids_carbone)
    df.columns = ["Article", "Libellé", "Poids carbone (kgCO2eq)"]

    # Create an SQLAlchemy engine to connect to the PostgreSQL database
    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    )

    # Write the DataFrame to the PostgreSQL table
    df.to_sql(
        "Poids_carbone_LTH",
        engine,
        schema="public",
        if_exists="replace",
        index=False,
    )

    connection.commit()

    # # SQL query to insert a new record into the table
    # query = "INSERT INTO public.\"Extraction_dates\" (Site, Type, Extraction_date) VALUES (%s, %s, %s);"

    # # Execute the query with the values
    # with connection.cursor() as cursor:
    #     cursor.execute(query, ("CLF", "Poids_carbone", extraction_date))
    # connection.commit()

    add_date_data(
        connection=connection,
        engine=engine,
        date=extraction_date,
        data_type="Poids_carbone",
        site="LTH",
    )
    st.rerun()


def upload_poids_carbone():
    """Add a file uploader for the poids carbone file.
    This function will add a file uploader to the Streamlit app, allowing the user to select an Excel file containing poids carbone data.
    It will also display the last date of poids carbone data.
    When the user selects a file and a date, and clicks the "Valider le chargement de poids carbone" button, the upload_poids_carbone_callback function will be called to process the data.
    """

    if "poids_carbon_file" not in st.session_state:
        st.session_state["poids_carbon_file"] = f"poids_carbon_file_key_0"
    poids_carbone = st.file_uploader(
        "Sélectionner un fichier Excel de poids carbone",
        key=st.session_state["poids_carbon_file"],
    )

    # SQL query to retrieve the last Extraction_date with filters
    # query = f"SELECT Extraction_date FROM public.\"Extraction_dates\" WHERE site = %s AND type = %s ORDER BY extraction_date DESC LIMIT 1;"
    # last_extraction_date = 0
    # try:
    #     with connection.cursor() as cursor:
    #         # Execute the query with the filter values
    #         cursor.execute(query, ("CLF", "Poids_carbone"))
    #         # Fetch the result
    #         last_extraction_date = cursor.fetchone()
    #         if len(last_extraction_date) >= 0:
    #             last_extraction_date = last_extraction_date[0]
    #         st.info(f"Date de la dernière extraction de poids carbone : {last_extraction_date.strftime('%d/%m/%Y')}")
    # except:
    #     # st.info("Aucune extraction de l'inventaire n'a été chargée dans la base de données.")
    #     st.info(f"Date de la dernière extraction de poids carbone: 04/10/2023")

    last_extraction_date = get_last_date(data_type="Poids_carbone")
    st.info(
        f"Date de la dernière extraction de poids carbone : {last_extraction_date.strftime('%d/%m/%Y')}"
    )

    if poids_carbone is not None:
        # Establish a connection to the PostgreSQL database
        connection, engine = get_connection()

        extraction_date = st.date_input(
            "Sélectionner la date de l'extraction",
            value=datetime.date.today(),
            max_value=datetime.date.today(),
            key="date_input_4",
        )

        if (
            last_extraction_date is None
            or last_extraction_date == 0
            or extraction_date >= last_extraction_date
        ):
            if st.button(
                "Valider le chargement de poids carbone",
                on_click=upload_poids_carbone_callback,
                args=(poids_carbone, connection, engine, extraction_date),
            ):
                st.session_state["poids_carbon_file"] = increment_key(
                    st.session_state["poids_carbon_file"]
                )
        else:
            st.info(
                "Les données plus récentes ont été déjà chargées dans la base de données."
            )

        connection.close()
        engine.dispose()


def add_sptgd():
    st.session_state.date = None

    if "reset" not in st.session_state:
        st.session_state.reset = False

    # st.title("Formulaire")
    if st.session_state.reset:
        st.session_state.date = None

    with st.form("sptgd"):
        st.subheader("Inserer des données dans la base SPTGD")

        # J'ai ajouté max_value pour ne pas dépasser la date du jour
        date = st.date_input(
            "Date", value=st.session_state.date, max_value=datetime.date.today()
        )

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

        # TODO : Afficher le dernier jour avec les données SPTGD disponibles
        # TODO : Afficher la liste des jours manquant les données SPTGD

    if submitted:
        st.session_state.reset = False
        try:
            connection, engine = get_connection()
            connect = connection.cursor()
            query = f'INSERT INTO public."SPTGD" ("Date", "Securite", "Taux de dispo", "Preventif", "Gmao centralise", "Demande intervention") VALUES (%s, %s, %s, %s , %s, %s)'
            # Je mets cette ligne en commentaire car ça induit une erreur pour moi
            # date = datetime.datetime.combine(date)
            format_date = date.strftime("%Y-%m-%d")
            data = [format_date, sec, Td, Pr, Gm, Inter]

            connect.execute(query, data)

            connection.commit()
            # TODO : après validation, remettre à 0 les valeurs
            # TODO : Afficher un message de succès pendant 3 secondes.

        except Exception as e:
            st.text(
                f"Attention : il y a une erreur au niveau des donnée que vous voulez insérer"
            )
            raise e
        finally:
            if connect:
                connect.close()
            if connection:
                connection.close()
            if engine:
                engine.dispose()

    if cancel:
        st.session_state.reset = True


def app():
    # add_evt_file()
    # add_inj_file()
    # add_qualite_file()
    # add_prod_file()
    # add_trafic_sortie_file()
    # upload_interventions()
    # upload_mvt_stock()
    # upload_inventaire()
    add_sptgd()
    # upload_poids_carbone()


def get_missing_dates(data_type, date_format=False):
    # SQL query with placeholder for data_type
    sql_query = """
    WITH All_Dates AS (
        SELECT DISTINCT "Date"
        FROM public."Dates_data"
        WHERE "Site" = 'LTH'
    ),
    Qualite_Dates AS (
        SELECT "Date"
        FROM public."Dates_data"
        WHERE "Data_type" = %s
        AND "Site" = 'LTH'
    ),
    lundi_samedi AS
    (
        WITH date_series AS (
            SELECT generate_series(
                '2023-01-01'::date,
                CURRENT_DATE - '1 day'::interval,
                '1 day'::interval
            ) AS date
        )
        SELECT date
        FROM date_series
		LEFT JOIN french_public_holidays ON date_series.date = french_public_holidays.holiday_date
        WHERE extract(dow FROM date) BETWEEN 1 AND 6
		AND french_public_holidays.holiday_date IS NULL
        ORDER BY date
    ),
    Reporting_Dates_1 AS (
        SELECT DISTINCT "Date"
        FROM public."LTH_Reporting"
    ),
    Reporting_Dates AS(
        SELECT 
        COALESCE(Reporting_Dates_1."Date", lundi_samedi."date") AS "Date"
        FROM Reporting_Dates_1
        FULL OUTER JOIN lundi_samedi ON Reporting_Dates_1."Date" = lundi_samedi."date"
    )
    SELECT 
        COALESCE(All_Dates."Date", Reporting_Dates."Date") AS "MergedDate"
    FROM All_Dates
    FULL OUTER JOIN Reporting_Dates ON All_Dates."Date" = Reporting_Dates."Date"
    LEFT JOIN Qualite_Dates ON All_Dates."Date" = Qualite_Dates."Date"
    WHERE Qualite_Dates."Date" IS NULL
    AND EXTRACT(DOW FROM COALESCE(All_Dates."Date", Reporting_Dates."Date")) <> 0
    ORDER BY "MergedDate" DESC
    """

    # Connect to the PostgreSQL database
    conn, _ = get_connection()
    cur = conn.cursor()

    # Execute the query with the data_type parameter
    cur.execute(sql_query, (data_type,))

    # Fetch the results
    if date_format:
        missing_dates = [row[0] for row in cur.fetchall()]
        missing_dates = [date for date in missing_dates if date.year >= 2023]
    else:
        missing_dates = cur.fetchall()
        missing_dates = [date for date in missing_dates if date[0].year >= 2023]
        if len(missing_dates) > 5:
            missing_dates = missing_dates[:5]
        missing_dates = ", ".join([x[0].strftime("%d/%m/%Y") for x in missing_dates])

    # Close the cursor and the connection
    cur.close()
    conn.close()

    return missing_dates


def upload_opb(OPB_file, connection, engine, date: datetime.date):
    """Upload the OPB file from Azure Blob Storage to the database.

    Args:
        OPB_file (UploadedFile): The uploaded OPB file.
        connection (psycopg2.extensions.connection): The database connection.
        engine (sqlalchemy.engine.base.Engine): The database engine.
        date (datetime.date): The date of the OPB data.
    """

    if OPB_file is None:
        OPB_file = get_Azure_file_bytes(
            f"PFC_LTH/0_raw_data/Extractions_quoti/{date.strftime('%Y%m%d')}/Evenementsetdefauts.xlsx"
        )
    if OPB_file is not None:

        query = (
            'SELECT "COEFF", "CLE_BOURRAGE" FROM public."Ponderations_Bourrages_LTH"'
        )
        # Fetch data into a DataFrame
        df_2 = pd.read_sql_query(query, connection)

        df_origine = pd.read_excel(
            OPB_file,
            skiprows=5,
            usecols=["Date heure de début", "Date heure de fin", "Machine", "Message"],
        )
        df = df_origine.loc[~df_origine["Message"].str.startswith("Fin :")]
        df = df.loc[df["Message"].isin(df_2["CLE_BOURRAGE"])]

        for date_col in ["Date heure de début", "Date heure de fin"]:
            df[date_col] = pd.to_datetime(df[date_col], dayfirst=True)

        # if len(df)>0:
        df["Date"] = df["Date heure de début"].iloc[0]
        df["Date"] = df["Date"].dt.date

        extraction_date = df["Date"].iloc[0]

        df_bourrage_iob = df_origine.loc[~df_origine["Message"].str.startswith("Fin :")]
        df_bourrage_iob = df_bourrage_iob.loc[
            (df_bourrage_iob["Message"].str.contains("Bourrage"))
            | (df_bourrage_iob["Message"].str.contains("Erreur IOB"))
        ]
        if len(df_bourrage_iob) != 0:
            df_bourrage_iob.loc[
                df_bourrage_iob["Message"].str.contains("Bourrage"), "Type"
            ] = "Bourrage"
            df_bourrage_iob.loc[
                df_bourrage_iob["Message"].str.contains("Erreur IOB"), "Type"
            ] = "IOB"
            df_bourrage_iob["Date"] = df_bourrage_iob["Date heure de début"].iloc[0]
            df_bourrage_iob["Date"] = df_bourrage_iob["Date"].dt.date

            df_bourrage_iob["Duree"] = (
                df_bourrage_iob["Date heure de fin"]
                - df_bourrage_iob["Date heure de début"]
            ).dt.total_seconds() / 3600
            df_bourrage_iob.drop_duplicates(subset=["Date heure de début", "Message"])

            df_bourrage_iob = (
                df_bourrage_iob.groupby(["Date", "Type"])[["Duree"]]
                .sum()
                .join(
                    df_bourrage_iob.groupby(["Date", "Type"])[["Duree"]]
                    .count()
                    .rename(columns={"Duree": "Nombre de défauts"})
                )
                .reset_index()
            )

        # Write the DataFrame to the PostgreSQL table
        if len(df_bourrage_iob) > 0:
            tuples_to_delete = [
                tuple(x) for x in df_bourrage_iob[["Date", "Type"]].to_numpy()
            ]
            table = "OPB_Bourrage_LTH"
            with connection.cursor() as cursor:
                try:
                    chunk_size = 100
                    # Create a list of chunks using a generator expression
                    chunks = [
                        tuples_to_delete[i : i + chunk_size]
                        for i in range(0, len(tuples_to_delete), chunk_size)
                    ]
                    for chunk in chunks:
                        cursor.execute(
                            f"""
                                DELETE FROM public."{table}"
                                WHERE ("Date", "Type") IN %s
                            """,
                            (tuple(chunk),),
                        )
                        connection.commit()
                except psycopg2.ProgrammingError:
                    pass

            df_bourrage_iob.to_sql(
                table,
                engine,
                schema="public",
                if_exists="append",
                index=False,
            )

        df_tmp = pd.merge(
            left=df,
            right=df_2,
            left_on="Message",
            right_on="CLE_BOURRAGE",
            how="inner",
        ).drop(columns=["CLE_BOURRAGE"])

        df_tmp["Duree_ponderee"] = (
            (
                df_tmp["Date heure de fin"] - df_tmp["Date heure de début"]
            ).dt.total_seconds()
            / 3600
            * df_tmp["COEFF"]
        )

        df_tmp.drop_duplicates(subset=["Date heure de début", "Message"])

        df_tmp = df_tmp.groupby("Date")[["Duree_ponderee"]].sum().reset_index()

        tuples_to_delete = [tuple(x) for x in df_tmp[["Date"]].to_numpy()]
        table = "OPB_LTH"
        with connection.cursor() as cursor:
            try:
                chunk_size = 100
                # Create a list of chunks using a generator expression
                chunks = [
                    tuples_to_delete[i : i + chunk_size]
                    for i in range(0, len(tuples_to_delete), chunk_size)
                ]
                for chunk in chunks:
                    cursor.execute(
                        f"""
                            DELETE FROM public."{table}"
                            WHERE ("Date") IN %s
                        """,
                        (tuple(chunk),),
                    )
                    connection.commit()
            except psycopg2.ProgrammingError:
                pass

        # Write the DataFrame to the PostgreSQL table
        df_tmp.to_sql(
            table,
            engine,
            schema="public",
            if_exists="append",
            index=False,
        )

        connection.commit()
        add_date_data(
            connection, engine, data_type="OPB", date=extraction_date, site="LTH"
        )


def upload_injection(date: datetime.date):
    injection_file_haut = get_Azure_file_bytes(
        f"PFC_LTH/0_raw_data/Extractions_quoti/{date.strftime('%Y%m%d')}/Injectiondescolisauxantennes_trieur_haut.xlsx",
    )

    if injection_file_haut is not None:
        connection, engine = get_connection()
        add_date_data(
            connection=connection,
            engine=engine,
            date=date,
            data_type="Injection_haut",
            site="LTH",
        )
        connection.close()
        engine.dispose()

    injection_file_bas = get_Azure_file_bytes(
        f"PFC_LTH/0_raw_data/Extractions_quoti/{date.strftime('%Y%m%d')}/Injectiondescolisauxantennes_trieur_bas.xlsx",
    )

    if injection_file_bas is not None:
        connection, engine = get_connection()
        add_date_data(
            connection=connection,
            engine=engine,
            date=date,
            data_type="Injection_bas",
            site="LTH",
        )
        connection.close()
        engine.dispose()

    if injection_file_haut is not None and injection_file_bas is not None:
        connection, engine = get_connection()

        df_haut = pd.read_excel(injection_file_haut)
        df_bas = pd.read_excel(injection_file_bas)

        try:
            total_haut = int(
                df_haut.loc[df_haut.Trieur == "Total"]["Total injecté"].iloc[0]
            )
        except:
            st.error(
                "Le format du fichier d'injection du trieur haut n'est pas bon. Merci de recharger le fichier."
            )
            time.sleep(3)
            return
        try:
            total_bas = int(
                df_bas.loc[df_haut.Trieur == "Total"]["Total injecté"].iloc[0]
            )
        except:
            st.error(
                "Le format du fichier d'injection du trieur bas n'est pas bon. Merci de recharger le fichier."
            )
            time.sleep(3)
            return

        total = total_haut + total_bas

        extraction_date = date.strftime("%Y-%m-%d")

        # SQL query to insert a new record into the table
        query = 'INSERT INTO public."Injection_par_jour_LTH" ("Date", "nombre de colis injectés") VALUES (%s, %s);'

        # Execute the query with the values
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                    DELETE FROM public."Injection_par_jour_LTH"
                    WHERE "Date"='{extraction_date}'
                """
            )
            connection.commit()
            cursor.execute(query, (extraction_date, total))
            connection.commit()

        connection.close()
        engine.dispose()


if __name__ == "__main__":

    # import dotenv

    # dotenv.load_dotenv()

    connection, engine = get_connection()
    date = datetime.date(2025, 4, 6)
    update_qualite_tri_data(connection, engine, date)

    print(get_last_date("Qualité_de_tri"))
    connection.close()
    engine.dispose()
    
    app()
    pass
    # start_date = datetime.date(2024, 7, 16)
    # end_date = datetime.date.today() - datetime.timedelta(days=1)
    # step = datetime.timedelta(days=1)
    # current_date = start_date
    # connection, engine = get_connection()
    # while current_date <= end_date:
    #     print(current_date)
    #     upload_injection(current_date)
    #     update_injections_antennes(connection, engine, current_date)
    #     current_date+=step
    # connection.close()
    # engine.dispose()

    # # Set the start date
    # # start_date = datetime.date(2023, 12, 1)
    # start_date = datetime.date(2024, 6, 1)
    # # start_date = datetime.date(2023, 6, 16)
    # # start_date = datetime.date(2024, 5, 29)
    # # start_date = datetime.date(2024, 5, 21)
    #
    # # Get today's date
    # end_date = datetime.date.today() - datetime.timedelta(days=1)
    # # end_date = datetime.date(2024, 3, 16)
    #
    # # Define the step for the loop (1 day)
    # step = datetime.timedelta(days=1)
    #
    # # Iterate over the dates
    # current_date = start_date
    #
    # # Establish a connection to the PostgreSQL database
    # connection = psycopg2.connect(
    #     host=host,
    #     port=port,
    #     dbname=dbname,
    #     user=user,
    #     password=password,
    #     sslmode=sslmode,
    # )
    #
    # while current_date <= end_date:
    #     # if current_date in get_missing_dates("Temps_fonctionnement", date_format=True):
    #     if True:
    #         print(current_date.strftime("%Y-%m-%d"))
    #         upload_opb(None, connection, current_date)  # OK
    #         # upload_injection(current_date)  # OK
    #         # update_evts_defauts(connection, current_date)  # OK
    #         # update_temps_fonctionnement(connection, current_date)  # OK
    #         update_injections_antennes(connection, current_date)  # OK
    #         # update_qualite_tri_data(connection, current_date)  # OK
    #     current_date += step
    # connection.close()