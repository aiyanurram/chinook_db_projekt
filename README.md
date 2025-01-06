# Dokumentacia k implementacii ETL procesu z databazy Chinook


Tento repozitar obsahuje implementaciu ETL procesu pre analyzu dat z databazy Chinook. Proces zahrna kroky na extrahovanie, transformovanie a nacitanie dat do dimenzionalneho modelu v Snowflake. Tento model podporuje vizualizaciu a analyzu udajov o albumoch, skladbach, zakaznikoch a predajoch.


## 1. Uvod a popis zdrojovych dat

Cielom tohto projektu bolo analyzovat data tykajuce sa skladieb, zakaznikov, hudobnych albumov a zoznamov skladieb. Tato analyza nam umoznuje identifikovat udaje o preferenciach hudby, najoblubenejsich skladbach, ako aj o spravani pouzivatelov pri interakcii s hudobnou platformou a nakupe hudby.

Odkaz na Chinook: [kliknite sem](https://github.com/lerocha/chinook-database)


### Zdrojove data:

- `album.csv`: Informacie o hudobnych albumoch.
- `artist.csv`: Informacie o artist.
- `customer.csv`: Informacie o zakaznikovi, ako aj kontaktne informacie zakaznika.
- `employee.csv`: Udaje o zamestnancoch, ktori spravuju objednavky.
- `genre.csv`: Hudobne zanre.
- `invoice.csv`: Faktury a informacie o predaji.
- `invoiceline.csv`: Informacie o poziciach vo fakturach.
- `mediatype.csv`: Typy medii.
- `playlist.csv:`: Zoznamy skladieb vytvorene pouzivatelmi.
- `playlisttrack.csv`: Prepojenie medzi zoznamami skladieb a skladbami.
- `track.csv`: Informacie o skladbe(nazvy, zanre a trvanie).

### 1.1 Datova architektura:

ERD diagram:

<p align="center">
  <img src="https://github.com/aiyanurram/chinook_db_projekt/blob/main/chinook_erd.png">
  <br>
  <em>]ERD schema Chinook</em>
</p>
