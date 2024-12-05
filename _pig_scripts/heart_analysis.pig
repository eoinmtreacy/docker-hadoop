SET mapreduce.job.complete.cancel.delegation.tokens false;

-- Load the data
heart_data = LOAD '/heart.csv' USING PigStorage(',')
    AS (id:int, age:int, sex:int, cp:int, trestbps:int, chol:int, fbs:int, restecg:int, thalach:int, exang:int, oldpeak:float, slope:int, ca:int, thal:int, target:int);

-- Group by age and calculate proportions
age_group = GROUP heart_data BY age;
age_proportion = FOREACH age_group {
    total = COUNT(heart_data);
    heart_disease = FILTER heart_data BY target == 1;
    no_heart_disease = FILTER heart_data BY target == 0;
    count_heart_disease = COUNT(heart_disease);
    count_no_heart_disease = COUNT(no_heart_disease);
    generate group AS age, 
             total, 
             count_heart_disease, 
             count_no_heart_disease, 
             (count_heart_disease / (float)total) AS proportion_heart_disease, 
             (count_no_heart_disease / (float)total) AS proportion_no_heart_disease;
};

-- Repeat for trestbps, chol, thalach, and oldpeak
trestbps_group = GROUP heart_data BY trestbps;
trestbps_proportion = FOREACH trestbps_group {
    total = COUNT(heart_data);
    heart_disease = FILTER heart_data BY target == 1;
    no_heart_disease = FILTER heart_data BY target == 0;
    count_heart_disease = COUNT(heart_disease);
    count_no_heart_disease = COUNT(no_heart_disease);
    generate group AS trestbps, 
             total, 
             count_heart_disease, 
             count_no_heart_disease, 
             (count_heart_disease / (float)total) AS proportion_heart_disease, 
             (count_no_heart_disease / (float)total) AS proportion_no_heart_disease;
};

chol_group = GROUP heart_data BY chol;
chol_proportion = FOREACH chol_group {
    total = COUNT(heart_data);
    heart_disease = FILTER heart_data BY target == 1;
    no_heart_disease = FILTER heart_data BY target == 0;
    count_heart_disease = COUNT(heart_disease);
    count_no_heart_disease = COUNT(no_heart_disease);
    generate group AS chol, 
             total, 
             count_heart_disease, 
             count_no_heart_disease, 
             (count_heart_disease / (float)total) AS proportion_heart_disease, 
             (count_no_heart_disease / (float)total) AS proportion_no_heart_disease;
};

thalach_group = GROUP heart_data BY thalach;
thalach_proportion = FOREACH thalach_group {
    total = COUNT(heart_data);
    heart_disease = FILTER heart_data BY target == 1;
    no_heart_disease = FILTER heart_data BY target == 0;
    count_heart_disease = COUNT(heart_disease);
    count_no_heart_disease = COUNT(no_heart_disease);
    generate group AS thalach, 
             total, 
             count_heart_disease, 
             count_no_heart_disease, 
             (count_heart_disease / (float)total) AS proportion_heart_disease, 
             (count_no_heart_disease / (float)total) AS proportion_no_heart_disease;
};

oldpeak_group = GROUP heart_data BY oldpeak;
oldpeak_proportion = FOREACH oldpeak_group {
    total = COUNT(heart_data);
    heart_disease = FILTER heart_data BY target == 1;
    no_heart_disease = FILTER heart_data BY target == 0;
    count_heart_disease = COUNT(heart_disease);
    count_no_heart_disease = COUNT(no_heart_disease);
    generate group AS oldpeak, 
             total, 
             count_heart_disease, 
             count_no_heart_disease, 
             (count_heart_disease / (float)total) AS proportion_heart_disease, 
             (count_no_heart_disease / (float)total) AS proportion_no_heart_disease;
};

-- Store the results
STORE age_proportion INTO '/data/out/age_proportion' USING PigStorage(',');
STORE trestbps_proportion INTO '/data/out/restbps_proportion' USING PigStorage(',');
STORE chol_proportion INTO '/data/out/chol_proportion' USING PigStorage(',');
STORE thalach_proportion INTO '/data/out/thalach_proportion' USING PigStorage(',');
STORE oldpeak_proportion INTO '/data/out/oldpeak_proportion' USING PigStorage(',');
