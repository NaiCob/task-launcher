import pandas as pd

from local_python_transformations.wspolne.funkcje import polacz_wybrane_kolumny, usun_wybrane_kolumny


def wylicz_kolumny_asortyment(df, df_asortyment, df_asortyment_join_col):
    df = df.merge(df_asortyment[['Przelicznik (szt)', 'ID Info', 'Nazwa MW', df_asortyment_join_col]],
                  on=df_asortyment_join_col, how='left')

    df['Przelicznik (szt)'].fillna(1, inplace=True)
    df['Przelicznik'] = df['Przelicznik (szt)']
    df['Przelicznik'] = df['Przelicznik'].replace('-', 1).astype("int64")
    df = usun_wybrane_kolumny(df, ['Przelicznik (szt)'])

    df['ID Info'].fillna('', inplace=True)
    df['ID MW'] = df['ID Info']
    df = usun_wybrane_kolumny(df, ['ID Info'])

    df['Nazwa MW'].fillna('', inplace=True)
    df['NAZWA MW'] = df['Nazwa MW']
    df = usun_wybrane_kolumny(df, ['Nazwa MW'])

    return df


def wylicz_kolumny_klient(df, df_klient, df_klient_join_col):
    df = polacz_wybrane_kolumny('ID_Pomocnicze_Klient', df,
                                ['Kod odbiorcy sklepu', 'Nazwa sklepu', 'Miejscowość', 'Adres', 'NIP'])
    df = df.merge(df_klient[[df_klient_join_col, 'ID']],
                  left_on='ID_Pomocnicze_Klient',
                  right_on=df_klient_join_col, how='left')
    df['ID LEW'] = df['ID']
    df = usun_wybrane_kolumny(df, ['ID', 'ID_Weryf.'])

    return df


def wylicz_standardowe_kolumny(df, df_asortyment=None, df_asortyment_join_col=None, df_klient=None,
                               df_klient_join_col=None, df_ceny=None):

    df['Wartość netto'] = df['Wartość sprzedaży netto'].replace('-', 0).astype("float")
    df = wylicz_kolumny_asortyment(df, df_asortyment, df_asortyment_join_col)
    df = wylicz_kolumny_klient(df, df_klient, df_klient_join_col)
    df['Ilość sprzedaży'] = pd.to_numeric(df['Ilość sprzedaży'], errors='coerce')
    df['Ilość_szt'] = df['Ilość sprzedaży'] * df['Przelicznik']
    # df['ID_Pomocnicze_Asortyment'] =
    # df['Spółka'] =
    # df['D.Wst.'] =
    # df['D.Wyst.'] =
    # df['SR'] =
    # df['Cena'] =
    # df['Pozycja'] =
    return df


def transformacje_specyficzne_dla_dostawcy(df):
    None
    return df
