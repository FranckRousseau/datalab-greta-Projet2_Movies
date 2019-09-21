# -*- coding: utf-8 -*-

#~ https://github.com/cernoch/movies.git

# executer avec :
#> python3 P2_CSV2SQLSP.py --base /home/perrot/Bureau/TEST --bdd BDD_Sondra?charset=utf8

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import argparse
#~ import creds
import os,configparser # credentials: 

parser = argparse.ArgumentParser()
#~ parser.add_argument("-f", action="store_true", help="traite FILMS")
#~ parser.add_argument("-p", action="store_true", help="traite People")
parser.add_argument("-v", action="store_true", help="Verbose SQL")
parser.add_argument("--base", help="Répertoire de movies")
parser.add_argument("--bdd", help="Base de donnée")
args = parser.parse_args()

config = configparser.ConfigParser()
config.read_file(open(os.path.expanduser("~/.datalab.cnf")))
print(config.sections())

base = args.base #"/home/perrot/Bureau/TEST"

DB=args.bdd #'BDD_Sondra?charset=utf8' # mySQL...
mySQLengine = create_engine("mysql://%s:%s@%s/%s" % (config['myBDD']['user'], config['myBDD']['password'], config['myBDD']['host'], DB), echo=args.v)

# Suppression des tables (dans le bon ordre)
# Vidage des tables : Film et People
mySQLengine.execute("DROP TABLE IF EXISTS FILMS ;")
mySQLengine.execute("SET FOREIGN_KEY_CHECKS = 0;");
mySQLengine.execute("DROP TABLE IF EXISTS People ;")
mySQLengine.execute("SET FOREIGN_KEY_CHECKS = 1;");
mySQLengine.execute("DROP TABLE IF EXISTS REMAKES ;")
mySQLengine.execute("SET FOREIGN_KEY_CHECKS = 1;");
mySQLengine.execute("DROP TABLE IF EXISTS CASTS ;")
mySQLengine.execute("SET FOREIGN_KEY_CHECKS = 1;");
mySQLengine.execute("DROP TABLE IF EXISTS ACTEURS ;")
mySQLengine.execute("SET FOREIGN_KEY_CHECKS = 1;");
mySQLengine.execute("DROP TABLE IF EXISTS CLIENTS ;")
mySQLengine.execute("SET FOREIGN_KEY_CHECKS = 1;");
mySQLengine.execute("DROP TABLE IF EXISTS LOCATION ;")
mySQLengine.execute("SET FOREIGN_KEY_CHECKS = 1;");

# Création, avec script. Un lien externe Film.producer -> People.ref
mySQLengine.execute("""
CREATE TABLE IF NOT EXISTS `BDD_Sondra`.`People` (
  `ref` VARCHAR(50) NOT NULL,
  `first` VARCHAR(45) NULL,
  `last` VARCHAR(45) NULL,
  `codes` VARCHAR(10) NULL,
  PRIMARY KEY (`ref`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_bin;
""")

mySQLengine.execute("""
CREATE TABLE IF NOT EXISTS `BDD_Sondra`.`FILMS` (
   FilmRef VARCHAR(10) NOT NULL,
   Film_Nom VARCHAR(60) NOT NULL,
   Film_Ann INT(4) NULL,
   FR_nom VARCHAR(50) NULL,
   FP_nom VARCHAR(50) NULL,
   FS_nom VARCHAR(50) NULL,
   Categorie VARCHAR(10) NULL,
  PRIMARY KEY (`FilmRef`),
  UNIQUE INDEX `FilmRef_UNIQUE` (`FilmRef` ASC))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_bin;
""")

mySQLengine.execute("""
CREATE TABLE IF NOT EXISTS `BDD_Sondra`.`REMAKES` (
    RemakeID VARCHAR(40) NOT NULL,
    OrigID VARCHAR(40) NOT NULL) 
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_bin;
""")

mySQLengine.execute("""
CREATE TABLE IF NOT EXISTS `BDD_Sondra`.`ACTEURS` (
    ActeurID INT NOT NULL AUTO_INCREMENT,
    Acteurs VARCHAR(60) NULL,
    Act_CDB INT(4) NULL,
    Act_CDF INT(4) NULL,
    Act_Prenoms VARCHAR(45) NULL,
    Act_Noms VARCHAR(45) NULL,
    Act_Sx VARCHAR(5) NULL,
    Act_DDN VARCHAR(45) NULL,
    Act_DDD VARCHAR(45) NULL,
    Act_Prf VARCHAR(60) NULL,
    Act_Orig VARCHAR(20) NULL,
    PRIMARY KEY (`ActeurID`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_bin;
""")

mySQLengine.execute("""
CREATE TABLE IF NOT EXISTS `BDD_Sondra`.`CASTS` (
   Film_Ref VARCHAR(10) NOT NULL,
   Film_Nom VARCHAR(100) NOT NULL,
   Acteurs VARCHAR(50) NULL,
   Pers VARCHAR(50) NULL,
   Role VARCHAR(200) NULL)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_bin;
""")

mySQLengine.execute("""
CREATE TABLE IF NOT EXISTS `BDD_Sondra`.`CLIENTS` (
    ClientID INT NOT NULL AUTO_INCREMENT,
    Cl_Nom VARCHAR(50) NOT NULL,
    Cl_Pren VARCHAR(50) NOT NULL,
    Cl_Sx CHAR(1) NOT NULL,
    Cl_DDN DATE NOT NULL,
    Cl_Tel VARCHAR(14) DEFAULT NULL,
    Cl_Em VARCHAR (50) NOT NULL UNIQUE,
    Cl_Prf VARCHAR(50) DEFAULT NULL,
    Cl_Fav VARCHAR(50) DEFAULT NULL,
    Cl_EC VARCHAR(5) DEFAULT NULL,
	Cl_Mbr VARCHAR(15) DEFAULT NULL,
	Cl_Date DATE,
    PRIMARY KEY (`ClientID`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_bin;
""")

mySQLengine.execute("""
CREATE TABLE IF NOT EXISTS `BDD_Sondra`.`LOCATION` (
   LocationID INT NOT NULL AUTO_INCREMENT,
   Loc_DB DATE DEFAULT NULL,
   Loc_DF DATE DEFAULT NULL,
   Client INT NOT NULL,
   Film INT NOT NULL,
   Mod_Pay VARCHAR(15) NOT NULL, 
   PRIMARY KEY (`LocationID`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8
COLLATE = utf8_bin;
""")

# lecture CSV
peopleCSV = pd.read_csv(base+"/people.csv", encoding = 'utf8', sep=',', header=None)
peopleCSV.columns = ('Ref_name', 'codes', 'Did', 'years', 'last_n', 'first_n', 'dob', 'dod') # FROM HTML

# le même, avec les colonnes qui nous intéresse
people = peopleCSV[['Ref_name', 'first_n', 'last_n', 'codes']] # Selection column
people.columns = ('ref', 'first', 'last', 'codes') # Rename to SQL
people.drop_duplicates(subset='ref', keep='first', inplace=True) # Drop duplicate for UNIQUE
people.set_index('ref', inplace=True) # nouvel index

if (True): #(args.f):
    # Les films
    FILMSCSV = pd.read_csv(base+"/FILMS.csv", encoding = 'utf8', sep=',')
    FILMSCSV.columns = ('FilmRef','Film_Nom', 'Film_Ann', 'FR_nom', 'FP_nom', 'FS_nom', 'Process', 'Categorie', 'F_Rcp')
    # le même, avec les colonnes qui nous intéresse
    FILMS = FILMSCSV[['FilmRef','Film_Nom', 'Film_Ann', 'FR_nom', 'FP_nom']]

    # Longueur title, producer: 45
    FILMS['Film_Nom'] = FILMS['Film_Nom'].str[:45] 
    FILMS['FP_nom'] = FILMS['FP_nom'].str[:45] # lien extern !
    FILMS['FR_nom'] = FILMS['FR_nom'].str[:45] # lien extern !
    
    # Les producers doivent exister dans people !
    FILMS['FP_nom'].replace('\s*,.*', '', inplace=True, regex=True)
    FILMS['FP_nom'].replace('P\w:.*', '', inplace=True, regex=True)
    FILMS['FP_nom'].replace('P:\s*', '', inplace=True, regex=True)

    FILMS['FR_nom'].replace('D:\s*', '', inplace=True, regex=True)

    print(FILMS)

    # Vérification de l'existance des liens externe...
    for x in FILMS['FP_nom']:
        if (x!="" and x not in people.index):
            # Si un producer n'esst pas dans l'index de people, on ajoute !
            print("+++ P:", x)
            people.loc[x] = [x, '', '?']

    for x in FILMS['FR_nom']:
        if (x!="" and x not in people.index):
            # Si un producer n'esst pas dans l'index de people, on ajoute !
            print("+++ D:", x)
            people.loc[x] = [x, '', '?']

    people.index = people.index.fillna('NA') # pas de ref vide...
    #print(people)
    
    FILMS['FP_nom'].replace(r'^\s*$', np.nan, regex=True, inplace=True) # remplacement reférence vide -> NaN pour SQL
    FILMS['FR_nom'].replace(r'^\s*$', np.nan, regex=True, inplace=True) # remplacement reférence vide -> NaN pour SQL

    #~ mySQLengine.execute("TRUNCATE People;# Wesh") # Empty table
    people.to_sql('People', mySQLengine, if_exists='append', index=True)

    #~ mySQLengine.execute("TRUNCATE Film;")
    FILMS.to_sql('FILMS', mySQLengine, if_exists='append', index=False)

    # Les Remakes
REMAKESCSV = pd.read_csv(base+"/REMAKESJ.csv", encoding = 'utf8', sep=',')
REMAKESCSV.columns = ('RemakeID','OrigID')
REMAKES = REMAKESCSV[['RemakeID','OrigID']]
print(REMAKES)
    
    #~ mySQLengine.execute("TRUNCATE REMAKES;")
REMAKES.to_sql('REMAKES', mySQLengine, if_exists='append', index=False)

    # Les Acteur
ACTEURSCSV = pd.read_csv(base+"/ACTEURS.csv", encoding = 'utf8', sep=',')
ACTEURSCSV.columns = ('ActeurID','Acteurs','Act_CDB','Act_CDF','Act_Prenoms','Act_Noms','Act_Sx','Act_DDN','Act_DDD','Act_Prf','Act_Orig')
ACTEURS = ACTEURSCSV[['ActeurID','Acteurs','Act_CDB','Act_CDF','Act_Prenoms','Act_Noms','Act_Sx','Act_DDN','Act_DDD','Act_Prf','Act_Orig']]
print(ACTEURS)

    #~ mySQLengine.execute("TRUNCATE ACTEURS;")
ACTEURS.to_sql('ACTEURS', mySQLengine, if_exists='append', index=False)

    # Les Casts
CASTSCSV = pd.read_csv(base+"/CASTS.csv", encoding = 'utf8', sep=',')
CASTSCSV.columns = ('Film_Ref','Film_Nom','Acteurs','Pers','Role')
CASTS = CASTSCSV[['Film_Ref','Film_Nom','Acteurs','Pers']]
print(CASTS)

    #~ mySQLengine.execute("TRUNCATE CASTS;")
CASTS.to_sql('CASTS', mySQLengine, if_exists='append', index=False) 

# Add chunksize=1 if errors - for more information.


    