#!/bin/bash

LIKES_PER_30_MIN_RATIO=30
INITIAL_TWEETS=10
COLECCION="test2"


POLITICOS_PP="@populares @ppcv @pablocasado_ @TeoGarciaEgea @Rafa_Hernando @DolorsMM @Ignacos @Albiol_XG @ALevySoler @IdiazAyuso"
POLITICOS_PSOE="@psoe @SocialistesVal @CristinaNarbona @sanchezcastejon @Adrilastra @Ander_Gil @patxilopez @susanadiaz @abalosmeco @ZaidaCantera"
POLITICOS_PODEMOS="@ahorapodemos @Podem_ @pablo_iglesias_ @Irene_Montero_ @pnique @TeresaRodr_ @ionebelarra @MayoralRafa @pbustinduy @MaribelMoraG"
POLITICOS_CS="@ciudadanosCS @CsValencia_C @albert_rivera @CiudadanoVille @GirautaOficial @InesArrimadas @begonavillacis @Tonicanto1 @jordi_canyas @FranHervias"
POLITICOS_VOX="@vox_es @VOX_VLC_Ciudad @santi_abascal @ortega_smith @monasterioR @ivanedlm"
POLITICOS_COMPROMIS="@compromis @CompromisVLC @enricmorera @monicaoltra @joanbaldovi @franferri_ @joanribo @perefuset @giuseppegrezzi"

python3 main_script.py -c $COLECCION -qu $POLITICOS_PP -p PP -mm $INITIAL_TWEETS
python3 main_script.py -c $COLECCION -qu $POLITICOS_PSOE -p PSOE -mm $INITIAL_TWEETS
python3 main_script.py -c $COLECCION -qu $POLITICOS_PODEMOS -p PODEMOS -mm $INITIAL_TWEETS
python3 main_script.py -c $COLECCION -qu $POLITICOS_CS -p CS -mm $INITIAL_TWEETS
python3 main_script.py -c $COLECCION -qu $POLITICOS_VOX -p VOX -mm $INITIAL_TWEETS
python3 main_script.py -c $COLECCION -qu $POLITICOS_COMPROMIS -p COMPROMIS -mm $INITIAL_TWEETS


python3 main_script.py -a -c $COLECCION
python3 main_script.py -c $COLECCION --likes -im $INITIAL_TWEETS -lr $LIKES_PER_30_MIN_RATIO
#python3 main_script.py -cu $COLECCION 