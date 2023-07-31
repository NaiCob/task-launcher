import pandas as pd

from local_python_transformations.wspolne.funkcje import polacz_wybrane_kolumny, usun_wybrane_kolumny


def wylicz_kolumny_asortyment(df, df_asortyment, df_asortyment_join_col):
    df = df.merge(df_asortyment[['Przelicznik', 'ID MW', 'Nazwa MW', df_asortyment_join_col]],
                  on=df_asortyment_join_col, how='left')

    df['Przelicznik'].fillna(1, inplace=True)
    df['Przelicznik'] = df['Przelicznik'].replace('-', 1).astype("int64")

    df['ID MW'].fillna('', inplace=True)

    df['Nazwa MW'].fillna('', inplace=True)
    df['NAZWA MW'] = df['Nazwa MW']
    df = usun_wybrane_kolumny(df, ['Nazwa MW'])

    return df


def wylicz_kolumny_klient(df, df_klient, df_klient_join_col):
    df = df.merge(df_klient[[df_klient_join_col, 'ID Info']],
                  left_on=['Odb_Identyfikator'],
                  right_on=df_klient_join_col, how='left')
    df['ID LEW'] = df['ID Info']
    df = usun_wybrane_kolumny(df, ['ID Info'])

    return df


def wylicz_standardowe_kolumny(df, df_asortyment=None, df_asortyment_join_col=None, df_klient=None,
                               df_klient_join_col=None, df_ceny=None):

    df['Wartość netto'] = df['Suma z Wartość netto'].replace('-', 0).astype("float")
    df = wylicz_kolumny_asortyment(df, df_asortyment, df_asortyment_join_col)
    df = wylicz_kolumny_klient(df, df_klient, df_klient_join_col)
    df['Suma z Ilość'] = pd.to_numeric(df['Suma z Ilość'], errors='coerce')
    df['Ilość_szt'] = df['Suma z Ilość'] * df['Przelicznik']

    return df

def transformacje_specyficzne_dla_dostawcy(df):
    None
    return df
