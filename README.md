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

## 2. Dimenzionalny model

Bol navrhnuty hviezdicovy model(star schema), kde centralnou tabulkou faktov je `sales_fact`, na ktoru sa pripajaju dalsie dimenzie:

- `dim_time` - datove udaje (den, mesiac, rok).
- `dim_employee` - data o employee (meno, priezvisko, vek a td).
- `dim_customer` - data o zakaznikov (meno, priezvisko, vek a td).
- `dim_track` - detaily o autorov, skladbach, skladateľov a td.

Pre relaciu medzi skladbami a objednavkami bola pouzita prepojovacia tabulka bridge_track_sales, ktora umoznuje spravne mapovanie relacie N:M medzi objednavkami a skladbami.

A este bolo pouzivane address tabulka medzi zamestnancom a klientom. Aby analyzovat predaj podla geografickych udajov (stat, krajina) a tiez znizit duplicitu udajov (adresy zakaznikov a zamestnancov).

<p align="center">
  <img src="https://github.com/aiyanurram/chinook_db_projekt/blob/main/star_version.png">
  <br>
  <em>Star verzia Chinook</em>
</p>

##3. ETL proces v Snowflake
ETL proces pozostaval z troch hlavnych faz: extrahovanie (Extract), transformacia (Transform) a nacitanie (Load). Tento proces bol implementovany v Snowflake s cielom pripravit zdrojove data zo staging vrstvy do viacdimenzionalneho modelu vhodneho na analyzu a vizualizaciu.

###3.1 Extract

`.csv` subory boli najprv nahrate pomocou interneho stage `my_stage`, co bolo realizovane prikazom:

```sql
CREATE OR REPLACE STAGE my_stage FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"');
```

Nasledujucim krokom bolo nahranie obsahu kazdeho `.csv` suboru do staging tabulky. 

1. Vytvorenie

```sql
create or replace TABLE track (
    trackid NUMBER(38,0),
    name VARCHAR(200),
    albumid NUMBER(38,0),
    mediatypeid NUMBER(38,0),
    genreid NUMBER(38,0),
    composer VARCHAR(220),
    milliseconds NUMBER(38,0),
    bytes NUMBER(38,0),
    unitprice NUMBER(10,2)
);
```

2. Importovanie
   
```sql
COPY INTO track
FROM @MY_STAGE/track.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);
```

3. Overenie operacii

```sql
SELECT * FROM track;
```

### 3.2 Transform

V tejto faze sa vykonavalo cistenie, transformacia a obohacovanie udajov zo staging tabuliek. Hlavnym cielom bolo pripravit dimenzie a faktovu tabulku na jednoduchu a efektivnu analyzu.

- `dim customer` - Tato tabulka poskytuje podrobnosti o zakaznikoch. Podla veku a pohlavia su ponechane ako NULL z dovodu chybajucich udajov.

```sql
CREATE OR REPLACE TABLE dim_customer AS
SELECT DISTINCT
    CustomerId,
    FirstName,
    LastName,
    Company,
    NULL AS Age,
    NULL AS Gender
FROM customer;
```

>**Typ dimenzie: SCD0 (Slowly Changing Dimensions - Povodna hodnota zostava nemenna)**<br>
>Demograficke udaje zakaznikov sa v tomto subore predpokladaju ako nemenne.

- `dim_track` - Obsahuje detaily o skladbach, vratane nazvov, autorov a playlistov.

```sql
CREATE OR REPLACE TABLE dim_track AS
SELECT DISTINCT
    TrackId,
    Name AS TrackName,
    Composer,
    Bytes,
    NULL AS Artist
FROM track;
```

>**Typ dimenzie: SCD0 (Slowly Changing Dimensions - Zachovanie povodnej hodnoty)**<br>
>Informacie o skladbach su v tomto datasete staticke.

- `dim_employee` - Obsahuje podrobnosti o zamestnancoch, vratane ich veku a narodnosti. Pohlavie je ponechane ako NULL z dovodu chybajucich udajov.

```sql
CREATE OR REPLACE TABLE dim_employee AS
SELECT DISTINCT
    EmployeeId AS EmployeeId,
    FirstName,
    LastName,
    DATEDIFF(YEAR, BirthDate, CURRENT_DATE) AS Age
FROM employee;
```

>**Typ dimenzie: SCD1 (Slowly Changing Dimensions - Prepisanie starej hodnoty)**<br>
>Udaje o zamestnancoch mozu byt aktualizovane podla potreby.

- `dim_time` - Extrahuje casove detaily, ako su hodiny, minuty a sekundy, z datumu faktury.

```sql
CREATE OR REPLACE TABLE dim_time AS
SELECT DISTINCT
    ROW_NUMBER() OVER (ORDER BY InvoiceDate) AS TimeId,
    EXTRACT(YEAR FROM CAST(InvoiceDate AS TIMESTAMP)) AS year,
    EXTRACT(MONTH FROM CAST(InvoiceDate AS TIMESTAMP)) AS month,
    EXTRACT(DAY FROM CAST(InvoiceDate AS TIMESTAMP)) AS day,
FROM invoice;
```

>**Typ dimenzie: SCD0 (Slowly Changing Dimensions - Zachovanie povodnej hodnoty)**<br>
>Informacie o case su nemenne.

- `sales_fact` - Konsoliduje transakcne udaje s metrikami, ako su Quantity, UnitPrice a Total. Obsahuje aj cudzie kluce spajajuce prislusne dimenzionalne tabulky.

```sql
CREATE OR REPLACE TABLE sales_fact AS
SELECT
    ROW_NUMBER() OVER (ORDER BY i.InvoiceId) AS Sales_factId,
    il.Quantity AS Quantity,
    il.UnitPrice AS UnitPrice,
    il.Quantity * il.UnitPrice AS Total,
    d.TimeId AS dim_time_TimeId,
    c.CustomerId AS dim_customer_CustomerId,
    e.EmployeeId AS dim_employee_EmployeeId,
    a.AddressId AS dim_address_AddressId,
    t.TrackId AS dim_track_TrackId
FROM
    invoice i
JOIN
    invoiceline il ON i.InvoiceId = il.InvoiceId
JOIN
    dim_customer c ON i.CustomerId = c.CustomerId
JOIN
    dim_employee e ON e.EmployeeId = e.EmployeeId
LEFT JOIN
    dim_address a ON i.BillingPostalCode = a.PostalCode
LEFT JOIN
    dim_time d ON DATE(i.InvoiceDate) = DATE(CONCAT(d.year, '-', d.month, '-', d.day))
JOIN
    dim_track t ON il.TrackId = t.TrackId;
```

>**Typ faktovej tabulky**: Additive Fact Table<br>
>Faktová tabuľka obsahuje metriky, ktoré je možné sčítať vo všetkých dimenziách (napr. Quantity, UnitPrice, Total).\
