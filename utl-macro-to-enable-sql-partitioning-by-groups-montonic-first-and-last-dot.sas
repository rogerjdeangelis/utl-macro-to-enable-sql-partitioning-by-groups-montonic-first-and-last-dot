%let pgm=utl-macro-to-enable-sql-partitioning-by-groups-montonic-first-and-last-dot;

A macro to enable sql partitioning  by groups in wps
Should be many applications for partitioning.

The wps macro below emulates how SQL partitioning.
  Emulate this sql statement in wps:  row_number() over (partition by sex, age) as partition
  This processing is trivial in the wps datastep, but not as simple in sql.

Note : Simpler in wps datastep. not so simple in SQL.

  PROBLEMS (this enacle top N by geoups in sql)

       1. wps sql top 2             Extract the fist two scores by sex and age in wps
       2  wps sql pivot wider       Transpose sinny to fat
       3  wps pivot wider sql array Transpose sinny to fat

Related reos on end

Although monotnic is undocumented, I think ot is ok in this context.

git hub
https://tinyurl.com/3c3vzps5
https://github.com/rogerjdeangelis/utl-macro-to-enable-sql-partitioning-by-groups-montonic-first-and-last-dot

macros
https://tinyurl.com/y9nfugth
https://github.com/rogerjdeangelis/utl-macros-used-in-many-of-rogerjdeangelis-repositories

/*         _ ____            _   _ _   _
 ___  __ _| |  _ \ __ _ _ __| |_(_) |_(_) ___  _ __    _ __ ___   __ _  ___ _ __ ___
/ __|/ _` | | |_) / _` | `__| __| | __| |/ _ \| `_ \  | `_ ` _ \ / _` |/ __| `__/ _ \
\__ \ (_| | |  __/ (_| | |  | |_| | |_| | (_) | | | | | | | | | | (_| | (__| | | (_) |
|___/\__, |_|_|   \__,_|_|   \__|_|\__|_|\___/|_| |_| |_| |_| |_|\__,_|\___|_|  \___/
        |_|
*/

/*----                                                                   ----*/
/*----  Save macro in autocall library                                   ----*/
/*----                                                                   ----*/

filename ft15f001 "c:/otowps/sqlPartition,sas";
parmcards4;
%macro sqlPartition(data,by=)/des="emulate sql partition over() funtionality";

  (select
     row_number
    ,row_number - min(row_number) +1 as partition
    ,*
  from
      (select *, monotonic() as row_number from
         /*----                                                          ----*/
         /*----  note max has no effect                                  ----*/
         /*----                                                          ----*/
         (select *, max(%scan(%str(&by),1,%str(,))) as sex from &data group by &by ))
  group
      by &by )

%mend sqlPartition;
;;;;
run;quit;

/*                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

options validvarname=upcase;
libname sd1 "d:/sd1";
data sd1.have;
  informat NAME $8. SEX $2.;
  input NAME$ SEX AGE SCORE;
cards4;
Alfred F 11  46
Alice F 11  45
Barbara F 11  53
Carol F 11  53
Henry F 12  61
James F 12  69
Jane F 12  57
Janet M 11  51
Jeffrey M 11  44
John M 11  53
Joyce M 11  53
Judy M 15  64
Louise M 15  77
Mary M 15  84
Philip M 15  64
Robert M 15  55
;;;;
run;quit;

/*     _              _           _      __ _          _     _
/ |___| |_   ___  ___| | ___  ___| |_   / _(_)_ __ ___| |_  | |___      _____
| / __| __| / __|/ _ \ |/ _ \/ __| __| | |_| | `__/ __| __| | __\ \ /\ / / _ \
| \__ \ |_  \__ \  __/ |  __/ (__| |_  |  _| | |  \__ \ |_  | |_ \ V  V / (_) |
|_|___/\__| |___/\___|_|\___|\___|\__| |_| |_|_|  |___/\__|  \__| \_/\_/ \___/

*/

 /**************************************************************************************************************************/
 /*                                            |                           |                                               */
 /*                                            |                           |                                               */
 /*                  INPUT                     |  PROCESS                  |                   OUTPUT                      */
 /* ROW                                        |                           |  ROW                                          */
 /* NUMBER  PARTITION   NAME   SEX AGE SCORE   |  Select the               |  NUMBER  PARTITION   NAME   SEX AGE SCORE     */
 /*                                            |  top two by sex and age   |                                               */
 /*  1          1      Alfred   F   11   46    |                           |   1          1      Alfred   F   11   46      */
 /*  2          2      Alice    F   11   45    |                           |   2          2      Alice    F   11   45      */
 /*  3          3      Barbara  F   11   53    |                           |                                               */
 /*  4          4      Carol    F   11   53    |                           |                                               */
 /*  5          1      Henry    F   12   61    |                           |   5          1      Henry    F   12   61      */
 /*  6          2      James    F   12   69    |                           |   6          2      James    F   12   69      */
 /*  7          3      Jane     F   12   57    |                           |                                               */
 /*  8          1      Janet    M   11   51    |                           |   8          1      Janet    M   11   51      */
 /*  9          2      Jeffrey  M   11   44    |                           |   9          2      Jeffrey  M   11   44      */
 /* 10          3      John     M   11   53    |                           |                                               */
 /* 11          4      Joyce    M   11   53    |                           |                                               */
 /* 12          1      Judy     M   15   64    |                           |  12          1      Judy     M   15   64      */
 /* 13          2      Louise   M   15   77    |                           |  13          2      Louise   M   15   77      */
 /* 14          3      Mary     M   15   84    |                           |                                               */
 /* 15          4      Philip   M   15   64    |                           |                                               */
 /* 16          5      Robert   M   15   55    |                           |                                               */
 /*                                            |                           |                                               */
 /**************************************************************************************************************************/

proc datasets lib=sd1 nolist nodetails;delete partition; run;quit;

%utl_submit_wps64x('
libname sd1 "d:/sd1";
options validvarname=any;
options sasautos=("c:/otowps");
proc sql;
 create
   table sd1.partition(where=(partition<3)) as
 select
   *
 from
   %sqlPartition(sd1.have,by=%str(sex, age));
;quit;
 proc print data=sd1.partition;
 run;quit;
');

/**************************************************************************************************************************/
/*                                                                                                                        */
/*  The WPS System                                                                                                        */
/*                                                                                                                        */
/*  Obs    row_number    partition     NAME      SEX    AGE    SCORE                                                      */
/*                                                                                                                        */
/*   1          1            1        Alfred      F      11      46                                                       */
/*   2          2            2        Alice       F      11      45                                                       */
/*   3          5            1        Henry       F      12      61                                                       */
/*   4          6            2        James       F      12      69                                                       */
/*   5          8            1        Janet       M      11      51                                                       */
/*   6          9            2        Jeffrey     M      11      44                                                       */
/*   7         12            1        Judy        M      15      64                                                       */
/*   8         13            2        Louise      M      15      77                                                       */
/*                                                                                                                        */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*___            _         _            _              _     _
|___ \ _ __   __| |  _ __ (_)_   _____ | |_  __      _(_) __| | ___ _ __
  __) | `_ \ / _` | | `_ \| \ \ / / _ \| __| \ \ /\ / / |/ _` |/ _ \ `__|
 / __/| | | | (_| | | |_) | |\ V / (_) | |_   \ V  V /| | (_| |  __/ |
|_____|_| |_|\__,_| | .__/|_| \_/ \___/ \__|   \_/\_/ |_|\__,_|\___|_|
                    |_|
*/


proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%utl_submit_wps64x('
options validvarname=any sasautos=("c:/otowps");
libname sd1 "d:/sd1";
proc sql;
    create
      table sd1.want as
    select
       sex
      ,age
      ,max(case when partition=1 then score else . end) as score1
      ,max(case when partition=2 then score else . end) as score2
      ,max(case when partition=3 then score else . end) as score3
      ,max(case when partition=4 then score else . end) as score4
      ,max(case when partition=5 then score else . end) as score6
   from
      %sqlPartition(sd1.have,by=%str(sex, age))
   group
      by sex, age
;quit;
');

proc print data=sd1.want;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* SEX    AGE    SCORE1    SCORE2    SCORE3    SCORE4    SCORE6                                                           */
/*                                                                                                                        */
/*  F      11      46        45        53        53         .                                                             */
/*  F      12      61        69        57         .         .                                                             */
/*  M      11      51        44        53        53         .                                                             */
/*  M      15      64        77        84        64        55                                                             */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*____                              _            _              _     _                       _
|___ /  __      ___ __  ___   _ __ (_)_   _____ | |_  __      _(_) __| | ___ _ __   ___  __ _| |   __ _ _ __ _ __ __ _ _   _
  |_ \  \ \ /\ / / `_ \/ __| | `_ \| \ \ / / _ \| __| \ \ /\ / / |/ _` |/ _ \ `__| / __|/ _` | |  / _` | `__| `__/ _` | | | |
 ___) |  \ V  V /| |_) \__ \ | |_) | |\ V / (_) | |_   \ V  V /| | (_| |  __/ |    \__ \ (_| | | | (_| | |  | | | (_| | |_| |
|____/    \_/\_/ | .__/|___/ | .__/|_| \_/ \___/ \__|   \_/\_/ |_|\__,_|\___|_|    |___/\__, |_|  \__,_|_|  |_|  \__,_|\__, |
                 |_|         |_|                                                           |_|                         |___/
*/

%array(_ix,values=1-5);

%put &=_ix2;
%put &=_ixn;

proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

%utl_submit_wps64x("
options validvarname=any sasautos=('c:/otowps');
libname sd1 'd:/sd1';
proc sql;
    create
      table sd1.want as
    select
       sex
      ,age
      ,%do_over(_ix,phrase=%str(
           max(case when partition=? then score else . end) as score?),between=comma)
   from
      %sqlPartition(sd1.have,by=%str(sex, age))
   group
      by sex, age
;quit;
");

proc print data=sd1.want;
run;quit;

/**************************************************************************************************************************/
/*                                                                                                                        */
/* SEX    AGE    SCORE1    SCORE2    SCORE3    SCORE4    SCORE6                                                           */
/*                                                                                                                        */
/*  F      11      46        45        53        53         .                                                             */
/*  F      12      61        69        57         .         .                                                             */
/*  M      11      51        44        53        53         .                                                             */
/*  M      15      64        77        84        64        55                                                             */
/*                                                                                                                        */
/**************************************************************************************************************************/

/*
 _ __ ___ _ __   ___  ___
| `__/ _ \ `_ \ / _ \/ __|
| | |  __/ |_) | (_) \__ \
|_|  \___| .__/ \___/|___/
         |_|
*/
https://github.com/rogerjdeangelis/utl-find-first-n-observations-per-category-using-proc-sql-partitioning
https://github.com/rogerjdeangelis/utl-transpose-pivot-wide-using-sql-partitioning-in-wps-r-python
https://github.com/rogerjdeangelis/utl-transposing-rows-to-columns-using-proc-sql-partitioning
https://github.com/rogerjdeangelis/utl_scalable_partitioned_data_to_find_statistics_on_a_column_by_a_grouping_variable
https://github.com/rogerjdeangelis/utl-using-sql-in-wps-r-python-select-the-four-youngest-male-and-female-students-partitioning



/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
