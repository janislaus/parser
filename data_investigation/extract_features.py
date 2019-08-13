# all exploration with stride = 100
# check relative number of nulls per column:
lst = []

for cl in df.columns:
    lst.append([cl, float(df.where(col(cl).isNotNull()).count())/float(df.count())])

lst.sort(key=lambda x: float(x[1]), reverse=True)

for item in lst:
    print(str(item[0]) + ": " + str(item[1]))

DELETION_DATE: 1.0 NO_DOC_OJS: 1.0 NO_OJ: 1.0 ORIGINAL_CPV: 1.0
ORIGINAL_CPV_CODE: 1.0 RECEPTION_ID: 1.0 TED_EXPORT_DOC_ID: 1.0
MA_MAIN_ACTIVITIES: 0.99673113593 MA_MAIN_ACTIVITIES_CODE: 0.99673113593 IA_URL_GENERAL: 0.950558430945
CPV_CODE_CODE: 0.682647779896 COUNTRY_VALUE: 0.664396622174 OFFICIALNAME: 0.664396622174
TOWN: 0.664396622174 POSTAL_CODE: 0.658744211387 E_MAIL: 0.657995096704
URL_GENERAL: 0.646485971125 DATE_DISPATCH_NOTICE: 0.561836011986 SHORT_DESCR: 0.561836011986
TITLE: 0.561836011986 TYPE_CONTRACT_CTYPE: 0.54923726505 VALUE: 0.521520021792
CONTACT_POINT: 0.474598202125 FAX: 0.45702805775 NOTICE_NUMBER_OJ: 0.441773358758
IA_URL_ETENDERING: 0.370607463906 DATE_CONCLUSION_CONTRACT: 0.349632252792 VAL_TOTAL: 0.331585399074
URL_BUYER: 0.316466902751 NB_TENDERS_RECEIVED: 0.311836011986 CA_TYPE_VALUE: 0.301757014437
NATIONALID: 0.294197766276 URL: 0.28657041678 MAIN_SITE: 0.274720784527
VAL_ESTIMATED_TOTAL: 0.168550803596 ORIGINAL_NUTS: 0.144170525742 ORIGINAL_NUTS_CODE: 0.144170525742
CA_CE_NUTS: 0.130345954781 NUTS_CODE: 0.110187959684 MONTH: 0.0943203486788
CA_TYPE_OTHER: 0.0705529828385 OPTIONS_DESCR: 0.0674203214383 DATE: 0.0660582947426
VALUE_COST: 0.0593162625987 VALUE_COST_FMTVAL: 0.0593162625987 LANGUAGE_VALUE: 0.0518251157723
TENDERER_NUTS: 0.0473985290112 TENDERER_NUTS_CODE: 0.0473985290112 DURATION: 0.0431762462544
DURATION_TYPE: 0.0431762462544 TI_DOC: 0.0313947153364 CRITERIA: 0.0305093979842
DATE_END: 0.0297602833016 DATE_START: 0.028670661945 DATE_PUBLICATION_NOTICE: 0.0228820484882
CPV_SUPPLEMENTARY_CODE_CODE: 0.00612912013075

# wer hat das tender bekommen?
AC_AWARD_CRIT: 1.0 AC_AWARD_CRIT_CODE: 1.0 ISO_COUNTRY_VALUE: 1.0
AWARD_CONTRACT_ITEM: 0.3721484508 CONTRACT_AWARD_LG: 0.0422199523323 CONTRACT_AWARD_FORM: 0.0422199523323
CONTRACT_AWARD_VERSION: 0.0422199523323 CONTRACT_AWARD_CATEGORY: 0.0422199523323
CONTRACT_AWARD_UTILITIES_CATEGORY: 0.00544773578481 CONTRACT_AWARD_UTILITIES_VERSION: 0.00544773578481
CONTRACT_AWARD_UTILITIES_FORM: 0.00544773578481 CONTRACT_AWARD_UTILITIES_FORM: 0.00544773578481
CONTRACT_AWARD_UTILITIES_LG: 0.00544773578481 CONTRACT_AWARD_DEFENCE_LG: 0.00170241743275
CONTRACT_AWARD_DEFENCE_FORM: 0.00170241743275 CONTRACT_AWARD_DEFENCE_VERSION: 0.00170241743275
FD_CONTRACT_AWARD_DEFENCE_CTYPE: 0.00170241743275

#sieht insgesamt schwierig aus hier. keine wirkliche daten dazu:
+------------------------+
|AC_AWARD_CRIT           |
+------------------------+
|Lowest price            |
|Other                   |
|The most economic tender|
|Mixed                   |
|Not applicable          |
|Not specified           |
+------------------------+

>>> df.select(col("AC_AWARD_CRIT_CODE")).distinct().show(100,False)
+------------------+
|AC_AWARD_CRIT_CODE|
+------------------+
|3                 |
|8                 |
|Z                 |
|9                 |
|1                 |
|2                 |
+------------------+

>>> df.select(col("AWARD_CONTRACT_ITEM")).distinct().show(100,False)
+-------------------+
|AWARD_CONTRACT_ITEM|
+-------------------+
|113                |
|null               |
|41                 |
|1                  |
|2                  |
+-------------------+

>>> df.select(col("CONTRACT_AWARD_LG")).distinct().show(100,False)
+-----------------+
|CONTRACT_AWARD_LG|
+-----------------+
|LT               |
|RO               |
|NL               |
|PL               |
|SV               |
|HR               |
|null             |
|PT               |
|EN               |
|DE               |
|ES               |
|FR               |
|ET               |
|IT               |
|LV               |
|EL               |
+-----------------+


# check consistency of cpv code columns

df2 = (df.select(col("CPV_CODE_CODE"),
        col("ORIGINAL_CPV_CODE")).where(col("CPV_CODE_CODE").isNotNull() &
        col("ORIGINAL_CPV_CODE").isNotNull()))
float(df2.where(col("CPV_CODE_CODE") == col("ORIGINAL_CPV_CODE")).count())/df2.count()

#--> 0.887988 also eine 90% übereinstimmung, dass ist nicht sonderlich gut
#- vielleicht nur ORIGINAL_CPV_CODE nehmen?

# how many of the tenders are with CPV_CODE 72...?
c1 = float(df.count())
c2 = float(df.withColumn("CPV_SHORT", df.ORIGINAL_CPV_CODE.substr(0,2)).where(col("CPV_SHORT") == "72").count())
c2/c1
#--> 0.033575 also ca 3% der tender werden in dem für uns interessanten bereich vergeben

# mit wievielen daten haben wir zu rechnen, wenn 1. Duration drin sein soll 2.
# der cpv_code mit 72 anfängt

c = (df.withColumn("CPV_SHORT", df.ORIGINAL_CPV_CODE.substr(0,2))
            .where((col("CPV_SHORT") == "72") & (col("DURATION").isNotNull()))
            .count())
#--> 23 zeilen oder 0.1%. was bedeuted das? Wenn die verteilung mit stride 1000
#eine gute approximation des gesamten merkmalsraums ist, dann haben wir bei ca
#1.5 Millionen Tenders nur ~2300 zeilen. wenn noch weitere einschränkungen dazu
#kommen, bleibt am ende nichts mehr übrig
#(z.b.: nur in deutschland vergebene tender, oder in welches ziel cluster, usw...
#die datenlage ist dann so dünn, dass man kaum noch von einer representativen
#sicht sprechen kann)


#wie kommt man an die Ziel-cluster: Justiz, Finanzen, Steuern, Soziale
#Sicherheit, Verwaltung, Rechenzentren
OJS = The Supplement to the Official Journal of the EU, dedicated to European
      public procurement
      --> das hilft nicht
RECEPTION_ID = Internal unique identification of the notice in the OP’s
               production system
      --> das hilft nicht

!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
>>> df.groupBy(col("MA_MAIN_ACTIVITIES")).count().show(100, False)
+----------------------------------------------------------------------+-----+
|MA_MAIN_ACTIVITIES                                                    |count|
+----------------------------------------------------------------------+-----+
|Water                                                                 |176  |
|Education                                                             |913  |
|Airport-related activities                                            |73   |
|Electricity                                                           |329  |
|Defence                                                               |370  |
|Extraction of gas and oil                                             |17   |
|Postal services                                                       |54   |
|Public order and safety                                               |366  |
|Economic and financial affairs                                        |291  |
|Production, transport and distribution of gas and heat                |117  |
|null                                                                  |48   |
|Social protection                                                     |247  |
|Urban railway / light rail, metro, tramway, trolleybus or bus services|221  |
|Health                                                                |2265 |
|Port-related activities / Maritime or inland waterway                 |36   |
|Railway services                                                      |361  |
|General public services                                               |5025 |
|Other                                                                 |2385 |
|Exploration and extraction of coal and other solid fuels              |64   |
|Not applicable                                                        |87   |
|Recreation, culture and religion                                      |167  |
|Environment                                                           |427  |
|Housing and community amenities                                       |556  |
|Not specified                                                         |89   |
+----------------------------------------------------------------------+-----+
#main activity of contracting body (auftraggeber)
#--> das ist eigentlich genau unsere zielclustercolumn:  Public order and safety, social protection, health?, general public services

#zusätzlich interessant
>>> df.groupBy(col("CA_TYPE_VALUE")).count().show(100, False)
+------------------+-----+
|CA_TYPE_VALUE     |count|
+------------------+-----+
|BODY_PUBLIC       |1439 |
|REGIONAL_AUTHORITY|2016 |
|null              |10253|
|EU_INSTITUTION    |33   |
|REGIONAL_AGENCY   |145  |
|NATIONAL_AGENCY   |137  |
|MINISTRY          |661  |
+------------------+-----+

#wohingegen das nicht wirklich hilft
>>> df.groupBy(col("CA_TYPE_OTHER")).count().orderBy("count", ascending=False).show(100, False)
+--------------------------------------------------------------------+-----+
|CA_TYPE_OTHER                                                       |count|
+--------------------------------------------------------------------+-----+
|null                                                                |13648|
|Samodzielny Publiczny Zakład Opieki Zdrowotnej                      |25   |
|Autre                                                               |24   |
|Établissement public de santé                                       |14   |
|EPCI                                                                |14   |
|Otras entidades del sector público                                  |13   |
|Collectivité territoriale                                           |10   |
|Uczelnia publiczna                                                  |9    |
|Établissement public de coopération intercommunale                  |8    |
|samodzielny publiczny zakład opieki zdrowotnej                      |8    |
|Ente público de derecho privado                                     |7    |
|státní podnik                                                       |7    |
|państwowa jednostka organizacyjna nieposiadająca osobowości prawnej |7    |
|Jednostka Wojskowa                                                  |7    |
|Państwowa osoba prawna                                              |7    |
|Otras Entidades del Sector Público                                  |7    |
|EPIC                                                                |7    |
|Forschungsgesellschaft e. V.                                        |7    |
|uczelnia publiczna                                                  |6    |
|Uczelnia Publiczna                                                  |6    |
|akciová společnost                                                  |6    |
|otras entidades del sector público                                  |6    |
|Santé                                                               |6    |
|Instytut Badawczy                                                   |6    |
|Akciová společnost                                                  |6    |
|SA d'HLM                                                            |6    |


#IA_URL_GENERAL = Main internet address (URL) of the contracting body
>>> df.groupBy(col("IA_URL_GENERAL")).count().orderBy("count", ascending=False).show(100, False)
+------------------------------------------------------------+-----+
|IA_URL_GENERAL                                              |count|
+------------------------------------------------------------+-----+
|null                                                        |726  |
|https://www.simap.ch                                        |180  |
|http://www.deutschebahn.com/bieterportal                    |95   |
|https://my.vergabe.bayern.de                                |89   |
|www.midlandsandlancashirecsu.nhs.uk                         |65   |
|https://contrataciondelestado.es                            |60   |
|http://www.arbeitsagentur.de                                |53   |
|http://www.haringey.gov.uk                                  |44   |
|https://www.etenders.gov.mt/epps                            |44   |
|http://www.marches-publics.gouv.fr                          |43   |
|www.walthamforest.gov.uk                                    |33   |
|www.iwight.com                                              |32   |
|www.cardiff.gov.uk                                          |28   |
|www.deutschebahn.com                                        |27   |
|www.sib.sachsen.de                                          |27   |
|https://etendersni.gov.uk/epps                              |27   |
|http://www.fraunhofer.de                                    |23   |
|https://www.marches-publics.gouv.fr                         |23   |
|http://www.deutschebahn.com/de/geschaefte/lieferantenportal |23   |
|http://www.madrid.org/contratospublicos                     |21   |
|http://www.evergabe-online.de/                              |21   |
|https://www.achatpublic.com/sdm/ent/gen/index.jsp           |21   |
|http://www.pgg.pl                                           |20   |
|http://www.marches-securises.fr                             |20   |
|http://www.rotterdam.nl                                     |20   |
|www.rosilva.ro                                              |19   |
|www.bip.gcm.pl                                              |17   |
|www.gddkia.gov.pl                                           |16   |
|https://www.rsd.cz                                          |16   |
|http://www.3rblog.wp.mil.pl                                 |16   |

#möglicherweise hiermit noch erweiterbar
>>> df.groupBy(col("URL_GENERAL")).count().orderBy("count", ascending=False).show(100, False)
+------------------------------------------------------------+-----+
|URL_GENERAL                                                 |count|
+------------------------------------------------------------+-----+
|null                                                        |5191 |
|http://www.deutschebahn.com/bieterportal                    |95   |
|https://www.simap.ch                                        |90   |
|www.midlandsandlancashirecsu.nhs.uk                         |65   |
|http://www.arbeitsagentur.de                                |52   |
|https://my.vergabe.bayern.de                                |48   |
|http://www.haringey.gov.uk                                  |44   |
|https://contrataciondelestado.es                            |40   |
|www.walthamforest.gov.uk                                    |33   |
|www.iwight.com                                              |32   |
|www.cardiff.gov.uk                                          |28   |
|www.deutschebahn.com                                        |27   |
|https://www.etenders.gov.mt/epps                            |24   |
|http://www.deutschebahn.com/de/geschaefte/lieferantenportal |23   |
|http://www.pgg.pl                                           |20   |
|www.sib.sachsen.de                                          |18   |
|https://www.rsd.cz                                          |16   |
|http://www.rotterdam.nl                                     |16   |
|https://www.eprocurement.gov.cy                             |15   |
|www.ratp.fr                                                 |15   |
|www.rosilva.ro                                              |15   |
|https://etendersni.gov.uk/epps                              |15   |
|http://www.chln.min-saude.pt                                |15   |
|http://www.madrid.org/contratospublicos                     |15   |
|www.gddkia.gov.pl                                           |14   |
