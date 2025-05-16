import datetime
import re
import pandas as pd
import streamlit as st
import numpy as np
import pickle


types = [
    "Apparition_plateau",
    "Disparition_bande",
    "Déchargement_plateau",
    "Déclenchement_plateau",
    "Bourrage",
    "Apparition_bande",
    "IOC",
    "NAT",
    "MTS",
    "TTS",
]

def get_corr_score(x, y):
    mX = sum(x) / len(x)
    mY = sum(y) / len(y)
    cov = sum((a - mX) * (b - mY) for (a, b) in zip(x, y)) / len(x)
    stdevX = (sum((a - mX) ** 2 for a in x) / len(x)) ** 0.5
    stdevY = (sum((b - mY) ** 2 for b in y) / len(y)) ** 0.5
    return round(cov/(stdevX*stdevY),3)

def get_R2_score(y,y_pred):
    mean_y = np.mean(y)
    ss_t = 0
    ss_r = 0
    for i in range(len(y)):  # val_count represents the no.of input x values
        ss_t += (y[i] - mean_y) ** 2
        ss_r += (y_pred[i] - y[i]) ** 2
    r2 = 1 - (ss_r / ss_t)
    return r2


def get_number_after_substring(s: str, subs: str):
    match = re.search(f"{subs}(\d+)", s)
    if match:
        return match.group(1)
    else:
        return ValueError


def get_word_after_substring(s: str, subs: str):
    return s.split(subs)[1].strip().split(" ")[0]


# @st.cache_data()
def get_evts_by_type(df_evt: pd.DataFrame, type_evt):
    data_clean = df_evt.copy()
    df_inj = pd.read_pickle("tmp_files/df_inj.pkl")
    df_inj = df_inj.fillna(0)
    # data_clean['Date heure de début'] = data_clean['Date heure de début'].apply(lambda x: x if (x.time().strftime("%H:%M:%S"))>="05:00:00" else x - datetime.timedelta(days = 1))
    # data_clean['Jour_de_la_semaine'] = data_clean['Date heure de début'].apply(lambda x: x.weekday())
    # data_clean['Heure_debut_defaut'] = data_clean['Date heure de début'].apply(lambda x: x.time().strftime("%H:%M:%S"))
    # data_clean['Heure'] = data_clean['Date heure de début'].apply(lambda x: x.hour)
    # data_clean["Date"] = data_clean['Date heure de début'].dt.date
    if type_evt == "Apparition_plateau":
        df_result = data_clean.loc[
            data_clean["Message"].str.lower().str.contains("apparition sur plateau")
        ][["Date", "Message", "# Occurrences"]]
        # for error in df_result.Message.unique():
        #     for date in data_clean["Date"].unique():
        #         if len(df_result.loc[(df_result.Message == error) & (df_result["Date"] == date)]) == 0:
        #             # df_result.loc[(df_result.Message == error) & (df_result["Date"] == date), "# Occurrences"] = 0
        #             new_row = {'Date': date, 'Message': error, '# Occurrences': 0}
        #             df_result = df_result.append(new_row, ignore_index = True)
        df_result["Plateau"] = df_result["Message"].apply(
            lambda x: get_number_after_substring(x, "plateau ")
        )
        df_result["SPS"] = pd.to_numeric(
            df_result["Message"]
            .str.lower()
            .apply(lambda x: get_number_after_substring(x, "sps ")), errors="coerce"
        )
        df_result = pd.merge(
            df_result,
            df_inj.groupby(["Date", "SPS"])[["Total injecté"]].sum().reset_index(),
            on=["Date", "SPS"],
            how="outer",
        )
    elif type_evt == "Déchargement_plateau":
        df_result = data_clean.loc[
            data_clean["Message"]
            .str.lower()
            .str.startswith("défaut de déchargement plateau")
        ][["Date", "Message", "# Occurrences"]]
        # for error in df_result.Message.unique():
        #     for date in data_clean["Date"].unique():
        #         if len(df_result.loc[(df_result.Message == error) & (df_result["Date"] == date)]) == 0:
        #             df_result.loc[(df_result.Message == error) & (df_result["Date"] == date), "# Occurrences"] = 0
        df_result["Plateau"] = (
            df_result["Message"]
            .str.lower()
            .apply(lambda x: get_number_after_substring(x, "plateau "))
        )
        df_result["Ilot"] = pd.to_numeric(
            df_result["Message"]
            .str.lower()
            .apply(lambda x: get_number_after_substring(x, "ilot ")), errors="coerce"
        )
        df_result["Sortie"] = (
            df_result["Message"]
            .str.lower()
            .apply(lambda x: get_number_after_substring(x, "sortie "))
        )
        df_result = pd.merge(
            df_result,
            df_inj.groupby(["Date", "SPS"])[["Total injecté"]].sum().reset_index(),
            left_on=["Date", "Ilot"],
            right_on=["Date", "SPS"],
            how="outer",
        ).drop(columns=["SPS"])
    elif type_evt == "Déclenchement_plateau":
        df_result = data_clean.loc[
            data_clean["Message"]
            .str.lower()
            .str.startswith("défaut de déclenchement plateau")
        ][["Date", "Message", "# Occurrences"]]
        # for error in df_result.Message.unique():
        #     for date in data_clean["Date"].unique():
        #         if len(df_result.loc[(df_result.Message == error) & (df_result["Date"] == date)]) == 0:
        #             df_result.loc[(df_result.Message == error) & (df_result["Date"] == date), "# Occurrences"] = 0
        df_result["Plateau"] = (
            df_result["Message"]
            .str.lower()
            .apply(lambda x: get_number_after_substring(x, "plateau "))
        )
        df_result["Ilot"] = pd.to_numeric(
            df_result["Message"]
            .str.lower()
            .apply(lambda x: get_number_after_substring(x, "ilot ")), errors="coerce"
        )
        df_result["Sortie"] = (
            df_result["Message"]
            .str.lower()
            .apply(lambda x: get_number_after_substring(x, "sortie "))
        )
        df_result = pd.merge(
            df_result,
            df_inj.groupby(["Date", "SPS"])[["Total injecté"]].sum().reset_index(),
            left_on=["Date", "Ilot"],
            right_on=["Date", "SPS"],
            how="outer",
        ).drop(columns=["SPS"])
    elif type_evt == "IOC":
        df_result = data_clean.loc[
            data_clean["Message"].str.startswith("Défaut Item-On-Cover")
        ][["Date", "Message", "# Occurrences"]]
        # for error in df_result.Message.unique():
        #     for date in data_clean["Date"].unique():
        #         if len(df_result.loc[(df_result.Message == error) & (df_result["Date"] == date)]) == 0:
        #             df_result.loc[(df_result.Message == error) & (df_result["Date"] == date), "# Occurrences"] = 0
        df_result["Plateau"] = (
            df_result["Message"]
            .str.lower()
            .apply(lambda x: get_number_after_substring(x, "plateau "))
        )
        df_result["Ilot"] = pd.to_numeric(
            df_result["Message"].apply(lambda x: get_number_after_substring(x, "IOC")), errors="coerce"
        )
        df_result = pd.merge(
            df_result,
            df_inj.groupby(["Date", "SPS"])[["Total injecté"]].sum().reset_index(),
            left_on=["Date", "Ilot"],
            right_on=["Date", "SPS"],
            how="outer",
        ).drop(columns=["SPS"])
    elif type_evt == "NAT":
        df_result = data_clean.loc[
            data_clean["Message"].str.lower().str.startswith("défaut plateau")
        ][["Date", "Message", "# Occurrences"]]
        # for error in df_result.Message.unique():
        #     for date in data_clean["Date"].unique():
        #         if len(df_result.loc[(df_result.Message == error) & (df_result["Date"] == date)]) == 0:
        #             df_result.loc[(df_result.Message == error) & (df_result["Date"] == date), "# Occurrences"] = 0
        df_result = df_result.loc[
            df_result["Message"].str.lower().str.contains("non aligné")
        ]
        df_result["Plateau"] = (
            df_result["Message"]
            .str.lower()
            .apply(lambda x: get_number_after_substring(x, "plateau "))
        )
        df_result["Ilot"] = pd.to_numeric(
            df_result["Message"]
            .str.lower()
            .apply(lambda x: get_number_after_substring(x, "nat")), errors="coerce"
        )
        # df_result["Ilot"] = df_result
        df_result = pd.merge(
            df_result,
            df_inj.groupby(["Date", "SPS"])[["Total injecté"]].sum().reset_index(),
            left_on=["Date", "Ilot"],
            right_on=["Date", "SPS"],
            how="outer",
        ).drop(columns=["SPS"])
    elif type_evt == "MTS":
        df_result = data_clean.loc[
            data_clean["Message"].str.lower().str.startswith("défaut plateau")
        ][["Date", "Message", "# Occurrences"]]
        # for error in df_result.Message.unique():
        #     for date in data_clean["Date"].unique():
        #         if len(df_result.loc[(df_result.Message == error) & (df_result["Date"] == date)]) == 0:
        #             df_result.loc[(df_result.Message == error) & (df_result["Date"] == date), "# Occurrences"] = 0
        df_result = df_result.loc[
            df_result["Message"]
            .str.lower()
            .str.contains("manquant détecté sur cellule")
        ]
        df_result["Plateau"] = (
            df_result["Message"]
            .str.lower()
            .apply(lambda x: get_number_after_substring(x, "plateau "))
        )
        df_result["Ilot"] = pd.to_numeric(
            df_result["Message"]
            .str.lower()
            .apply(lambda x: get_number_after_substring(x, "mts")), errors="coerce"
        )
        df_result = pd.merge(
            df_result,
            df_inj.groupby(["Date", "SPS"])[["Total injecté"]].sum().reset_index(),
            left_on=["Date", "Ilot"],
            right_on=["Date", "SPS"],
            how="outer",
        ).drop(columns=["SPS"])
    elif type_evt == "TTS":
        df_result = data_clean.loc[
            data_clean["Message"].str.lower().str.startswith("défaut plateau")
        ][["Date", "Message", "# Occurrences"]]
        # for error in df_result.Message.unique():
        #     for date in data_clean["Date"].unique():
        #         if len(df_result.loc[(df_result.Message == error) & (df_result["Date"] == date)]) == 0:
        #             df_result.loc[(df_result.Message == error) & (df_result["Date"] == date), "# Occurrences"] = 0
        df_result = df_result.loc[
            df_result["Message"].str.lower().str.contains("basculé sur")
        ]
        df_result["Plateau"] = (
            df_result["Message"]
            .str.lower()
            .apply(lambda x: get_number_after_substring(x, "plateau "))
        )
        df_result["Ilot"] = pd.to_numeric(
            df_result["Message"]
            .str.lower()
            .apply(lambda x: get_number_after_substring(x, "tts")), errors="coerce"
        )
        df_result = pd.merge(
            df_result,
            df_inj.groupby(["Date", "SPS"])[["Total injecté"]].sum().reset_index(),
            left_on=["Date", "Ilot"],
            right_on=["Date", "SPS"],
            how="outer",
        ).drop(columns=["SPS"])
    elif type_evt == "Bourrage":
        df_result = data_clean.loc[
            data_clean["Message"].str.lower().str.startswith("défaut de bourrage")
        ][["Date", "Message", "# Occurrences"]]
        # for error in df_result.Message.unique():
        #     for date in data_clean["Date"].unique():
        #         if len(df_result.loc[(df_result.Message == error) & (df_result["Date"] == date)]) == 0:
        #             df_result.loc[(df_result.Message == error) & (df_result["Date"] == date), "# Occurrences"] = 0
        df_result["Injecteur"] = pd.to_numeric(
            df_result["Message"]
            .str.lower()
            .apply(lambda x: get_number_after_substring(x, "injecteur ")), errors="coerce"
        )
        df_result["Cellule"] = (
            df_result["Message"]
            .str.lower()
            .apply(lambda x: get_word_after_substring(x, "cellule"))
        )
        df_result = pd.merge(
            df_result,
            df_inj.groupby(["Date", "Antenne"])[["Total injecté"]].sum().reset_index(),
            left_on=["Date", "Injecteur"],
            right_on=["Date", "Antenne"],
            how="outer",
        ).drop(columns=["Antenne"])
    elif type_evt == "Disparition_bande":
        df_result = data_clean.loc[
            data_clean["Message"]
            .str.lower()
            .str.startswith("défaut disparition sur la bande")
        ][["Date", "Message", "# Occurrences"]]
        # for error in df_result.Message.unique():
        #     for date in data_clean["Date"].unique():
        #         if len(df_result.loc[(df_result.Message == error) & (df_result["Date"] == date)]) == 0:
        #             df_result.loc[(df_result.Message == error) & (df_result["Date"] == date), "# Occurrences"] = 0
        df_result["Injecteur"] = pd.to_numeric(
            df_result["Message"]
            .str.lower()
            .apply(lambda x: get_number_after_substring(x, "injecteur ")), errors="coerce"
        )
        df_result = pd.merge(
            df_result,
            df_inj.groupby(["Date", "Antenne"])[["Total injecté"]].sum().reset_index(),
            left_on=["Date", "Injecteur"],
            right_on=["Date", "Antenne"],
            how="outer",
        ).drop(columns=["Antenne"])
    else:
        df_result = data_clean.loc[
            data_clean["Message"]
            .str.lower()
            .str.startswith("défaut apparition sur la bande")
        ][["Date", "Message", "# Occurrences"]]
        # for error in df_result.Message.unique():
        #     for date in data_clean["Date"].unique():
        #         if len(df_result.loc[(df_result.Message == error) & (df_result["Date"] == date)]) == 0:
        #             df_result.loc[(df_result.Message == error) & (df_result["Date"] == date), "# Occurrences"] = 0
        df_result["Injecteur"] = pd.to_numeric(
            df_result["Message"]
            .str.lower()
            .apply(lambda x: get_number_after_substring(x, "injecteur ")), errors="coerce"
        )
        df_result = pd.merge(
            df_result,
            df_inj.groupby(["Date", "Antenne"])[["Total injecté"]].sum().reset_index(),
            left_on=["Date", "Injecteur"],
            right_on=["Date", "Antenne"],
            how="outer",
        ).drop(columns=["Antenne"])

    return df_result


def clean_data(df_evt):
    data_clean = df_evt.copy()
    data_clean = data_clean[
        (data_clean["Machine"] != "354050ZP0005")
        & (data_clean["Machine"] != "354050ZL0001")
        & (data_clean["Machine"] != "TPGD-REN-ARC")
        & (data_clean["Machine"] != "API API Non-Meca")
        & (data_clean["Machine"] != "TPGD-REN-SUP1")
        & (data_clean["Machine"] != "TPGD-REN-SUP2")
        & (data_clean["Machine"] != "PFC-REN-PNM1")
        & (data_clean["Machine"] != "PFC-REN-PNM2")
    ]
    data_clean["Date heure de début"] = data_clean["Date heure de début"].apply(
        lambda x: x
        if (x.time().strftime("%H:%M:%S")) >= "05:00:00"
        else x - datetime.timedelta(days=1)
    )
    data_clean["Jour_de_la_semaine"] = data_clean["Date heure de début"].apply(
        lambda x: x.weekday()
    )
    data_clean["Heure_debut_defaut"] = data_clean["Date heure de début"].apply(
        lambda x: x.time().strftime("%H:%M:%S")
    )
    data_clean["Heure"] = data_clean["Date heure de début"].apply(lambda x: x.hour)
    data_clean = data_clean[
        ~data_clean["Message"].str.contains("Douchette")
    ].reset_index(drop=True)
    data_clean = data_clean[~data_clean["Message"].str.contains("connexion")]
    data_clean = data_clean[~data_clean["Message"].str.contains("tâche")]
    data_clean = data_clean[~data_clean["Message"].str.lower().str.contains("mode")]
    data_clean = data_clean[~data_clean["Message"].str.contains("Sql")].reset_index(
        drop=True
    )
    data_clean["Message_clean"] = data_clean["Message"]
    data_clean["Message_clean"] = data_clean["Message_clean"].apply(
        lambda x: re.sub(r"plateau \d+", "plateau", x).strip()
    )
    data_clean["Message_clean"] = data_clean["Message_clean"].apply(
        lambda x: re.sub(r"sortie \d+ et \d+", "sortie", x.lower()).strip()
    )
    data_clean["Message_clean"] = data_clean["Message_clean"].apply(
        lambda x: re.sub(r"sortie \d+_\d+", "sortie", x.lower()).strip()
    )
    data_clean["Message_clean"] = data_clean["Message_clean"].apply(
        lambda x: re.sub(r"sortie \d+", "sortie", x.lower()).strip()
    )
    data_clean["Message_clean"] = data_clean["Message_clean"].apply(
        lambda x: re.sub(r"glacis \d+", "glacis", x.lower()).strip()
    )
    data_clean["Message_clean"] = data_clean["Message_clean"].apply(
        lambda x: re.sub(r"convoyeur \d+", "convoyeur", x.lower()).strip()
    )
    data_clean["Message_clean"] = data_clean["Message_clean"].apply(
        lambda x: re.sub(r"connecteur \d+", "connecteur", x.lower()).strip()
    )
    data_clean["Message_clean"] = data_clean["Message_clean"].apply(
        lambda x: re.sub(r"flap \d+", "flap", x.lower()).strip()
    )
    data_clean["Message_clean"] = data_clean["Message_clean"].apply(
        lambda x: re.sub(r"quai \d+", "quai", x.lower()).strip()
    )
    data_clean["Message_clean"] = data_clean["Message_clean"].apply(
        lambda x: re.sub(r"caljan \d+", "caljan", x.lower()).strip()
    )
    data_clean["Message_clean"] = data_clean["Message_clean"].apply(
        lambda x: re.sub(r"bf\d+", "bf", x.lower()).strip()
    )
    data_clean["Message_clean"] = data_clean["Message_clean"].apply(
        lambda x: re.sub(r"pic \d+", "pic", x.lower()).strip()
    )
    data_clean["Message_clean"] = data_clean["Message_clean"].apply(
        lambda x: re.sub(r"urgence au\d+", "urgence au", x.lower()).strip()
    )

    data_clean["Temps_def"] = (
        data_clean["Date heure de fin"] - data_clean["Date heure de début"]
    )
    data_clean["Temps_def"] = data_clean["Temps_def"].dt.seconds
    data_clean["Temps_def_delta"] = pd.to_timedelta(data_clean["Temps_def"], unit="S")

    data_clean["Equipe"] = "Temp"

    data_clean.loc[
        (
            (data_clean["Jour_de_la_semaine"] > 0)
            & (data_clean["Heure_debut_defaut"] < "12:30:00")
        )
        & (
            (data_clean["Jour_de_la_semaine"] > 0)
            & (data_clean["Heure_debut_defaut"] > "05:00:00")
        ),
        "Equipe",
    ] = "Matin"

    data_clean.loc[
        (
            (data_clean["Jour_de_la_semaine"] < 5)
            & (data_clean["Heure_debut_defaut"] < "19:30:00")
        )
        & (
            (data_clean["Jour_de_la_semaine"] < 5)
            & (data_clean["Heure_debut_defaut"] > "12:30:00")
        ),
        "Equipe",
    ] = "Apres-midi"

    data_clean.loc[
        (
            (data_clean["Jour_de_la_semaine"] == 5)
            & (data_clean["Heure_debut_defaut"] < "20:30:00")
        )
        & (
            (data_clean["Jour_de_la_semaine"] == 5)
            & (data_clean["Heure_debut_defaut"] > "12:30:00")
        ),
        "Equipe",
    ] = "Apres-midi"

    data_clean.loc[
        (
            (data_clean["Jour_de_la_semaine"] < 5)
            & (data_clean["Heure_debut_defaut"] > "19:30:00")
        )
        | (
            (data_clean["Jour_de_la_semaine"] > 0)
            & (data_clean["Heure_debut_defaut"] < "05:00:00")
        ),
        "Equipe",
    ] = "Soir"

    data_clean.loc[
        (data_clean["Jour_de_la_semaine"] == 0)
        & (data_clean["Heure_debut_defaut"] < "12:30:00"),
        "Equipe",
    ] = "Normalement pas de défaut"
    data_clean = data_clean.loc[
        ~data_clean["Message"].str.contains("Fin :")
    ].reset_index(drop=True)
    return data_clean, len(data_clean)


def get_nb_defaults(filename: str) -> int:
    col_int = [1, 4, 5, 6]
    df_evt_tmp = pd.read_excel(filename, usecols=col_int, skiprows=5)
    df_evt_clean, nb_evts = clean_data(df_evt_tmp)
    df_evt_clean["Date"] = df_evt_clean["Date heure de début"].dt.date()
    return df_evt_clean.groupby("Date").size()


def get_prediction_injections(filename: str) -> pd.DataFrame:
    df_inj = pd.read_excel(filename, header=[0, 1])
    return df_inj
