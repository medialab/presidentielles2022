echo "main"
gazou export --since 2022-03-22 --until 2022-04-25 "pr?sidentielle* OR ?l?ction*2022 OR ?lys?e2022 OR candidat*2022" --lucene --step days > presidentielle_main_20220322-20220425.csv
echo "pécresse"
gazou export --since 2022-03-22 --until 2022-04-25 "*p?cresse* OR lesr?publicains OR avecvalerie OR SoyonsLibres OR Jeunes_avec_VP" --lucene --step days > presidentielle_pecresse_20220322-20220425.csv
echo "macron"
gazou export --since 2022-03-22 --until 2022-04-25 "*macron* OR *enmarche*" --lucene --step days > presidentielle_macron_20220322-20220425.csv
echo "hidalgo"
gazou export --since 2022-03-22 --until 2022-04-25 "*hidalgo* OR partisocialiste" --lucene --step days > presidentielle_hidalgo_20220322-20220425.csv
echo "roussel"
gazou export --since 2022-03-22 --until 2022-04-25 "*roussel* OR pcf" --lucene --step days > presidentielle_roussel_20220322-20220425.csv
echo "arthaud"
gazou export --since 2022-03-22 --until 2022-04-25 "*arthaud* OR \"lutte ouvri?re\" OR LutteOuvri?re" --lucene --step days > presidentielle_arthaud_20220322-20220425.csv
echo "lassalle"
gazou export --since 2022-03-22 --until 2022-04-25 "*lassalle* OR JeunesAvecJL OR R?sistonsFrance" --lucene --step days > presidentielle_lassalle_20220322-20220425.csv
echo "jadot"
gazou export --since 2022-03-22 --until 2022-04-25 "*jadot* OR eelv" --lucene --step days > presidentielle_jadot_20220322-20220425.csv
echo "dupontaignan"
gazou export --since 2022-03-22 --until 2022-04-25 "*dupontaignan* OR dupont-aignan OR DLF_Officiel" --lucene --step days > presidentielle_dupontaignan_20220322-20220425.csv
echo "lepen"
gazou export --since 2022-03-22 --until 2022-04-25 "*lepen* OR mlafrance OR mlp_officiel OR rnational_off OR marine2022" --lucene --step days > presidentielle_lepen_20220322-20220425.csv
echo "zemmour"
gazou export --since 2022-03-22 --until 2022-04-25 "*zemmour* OR Reconquete2022 OR generationZ_off OR reconquete_z OR Z2022" --lucene --step days > presidentielle_zemmour_20220322-20220425.csv
echo "asselineau"
gazou export --since 2022-03-22 --until 2022-04-25 "*asselineau* OR fa22 OR UPR OR generationA OR LaFranceNotreAvenir" --lucene --step days > presidentielle_asselineau_20220322-20220425.csv
echo "poutou"
gazou export --since 2022-03-22 --until 2022-04-25 "*poutou* OR NPA_officiel" --lucene --step days > presidentielle_poutou_20220322-20220425.csv
echo "kazib"
gazou export --since 2022-03-22 --until 2022-04-25 "*kazib* OR RevPermanente" --lucene --step days > presidentielle_kazib_20220322-20220425.csv
echo "taubira"
gazou export --since 2022-03-22 --until 2022-04-25 "*taubira* OR PrimairePop" --lucene --step days > presidentielle_taubira_20220322-20220425.csv
echo "thouy"
gazou export --since 2022-03-22 --until 2022-04-25 "*thouy* OR \"parti animaliste\" OR partianimaliste" --lucene --step days > presidentielle_thouy_20220322-20220425.csv
echo "kuzmanovic"
gazou export --since 2022-03-22 --until 2022-04-25 "*kuzmanovic* OR r?publiquesouveraine OR vukuzman OR RSouveraine" --lucene --step days > presidentielle_kuzmanovic_20220322-20220425.csv
echo "koenig"
gazou export --since 2022-03-22 --until 2022-04-25 "\"gaspard koenig\" OR Koenig2022 OR MouvementSimple" --lucene --step days > presidentielle_koenig_20220322-20220425.csv
echo "egger"
gazou export --since 2022-03-22 --until 2022-04-25 "espoirric2022 OR ClaraEgger1 OR \"clara egger\"" --lucene --step days > presidentielle_egger_20220322-20220425.csv
echo "mélenchon"
gazou export --since 2022-03-22 --until 2022-04-25 "*m?l?nchon* OR lfi OR unionpopulaire OR \"union populaire\" OR FranceInsoumise OR avecjlm OR jlm2022" --lucene --step days > presidentielle_melenchon_20220322-20220425.csv
