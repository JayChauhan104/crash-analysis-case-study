from pyspark.sql.functions import col, count, countDistinct, row_number, collect_set, array_contains, rank, regexp_extract
from pyspark.sql.window import Window

class CrashAnalysis:
    """
    A class for performing various crash analysis using Spark DataFrame.

    Attributes:
        df_store: An instance of DFStore class for accessing DataFrames.
    """
    def __init__(self, df_store):
        """
        Initialize CrashAnalysis with a DataFrame store.

        Parameters:
            df_store: An instance of DFStore.
        """
        if not df_store:
            raise Exception("Dataframe store is required for supported analysis")
        self.__df_store = df_store

    def analyse_fatal_male_crashes(self):
        """
        Find the number of crashes (accidents) in which number of males killed are greater than 2.

        Returns:
            DataFrame or None: The DataFrame with crash count or None if error.
            str or None: An error message if DataFrame retrieval fails, else None.
        """
        primary_person_df, err = self.__df_store.get_df("primary_person")
        
        if err:
            return None, err
        
        num = primary_person_df \
        .filter((col("PRSN_INJRY_SEV_ID") == "KILLED") & (col("PRSN_GNDR_ID") == "MALE")) \
        .groupBy("CRASH_ID") \
        .agg(countDistinct("CRASH_ID").alias("CRASH_COUNT_WITH_GREATER_THAN_TWO_MALES")) \
        .filter(col("CRASH_COUNT_WITH_GREATER_THAN_TWO_MALES") > 2) \
        .select(col("CRASH_COUNT_WITH_GREATER_THAN_TWO_MALES"))
    
        return num, None

    def count_two_wheeler_crashes(self):
        """
        Count the number of crashes involving two-wheelers.

        Returns:
            DataFrame or None: The DataFrame with the count of two-wheelers involved in crashes,
                            or None if an error occurs.
            str or None: An error message if DataFrame retrieval fails, else None.
        """
        units_df, err = self.__df_store.get_df("units")
        if err:
                return None, err
        
        charges_df, err = self.__df_store.get_df("charges")
        if err:
                return None, err
        
        motorcycle_df = charges_df.join(units_df.filter(col("VEH_BODY_STYL_ID") == "MOTORCYCLE").select("CRASH_ID", "UNIT_NBR"), ["CRASH_ID", "UNIT_NBR"], "inner") \
        .agg(countDistinct("UNIT_NBR").alias("TWO_WHEELERS_BOOKED_FOR_CRASH"))

        return motorcycle_df, None

    def top_vehicle_makes_driver_fatal_no_airbags(self):
        """
        Determines the Top 5 Vehicle Makes of the cars present in the crashes in which driver died and Airbags did not deploy.

        Returns:
            DataFrame or None: The DataFrame with the top 5 vehicle makes and their counts, or None if an error occurs.
            str or None: An error message if DataFrame retrieval fails, else None.
        """
        units_df, err = self.__df_store.get_df("units")
        if err:
                return None, err
        
        primary_person_df, err = self.__df_store.get_df("primary_person")
        if err:
                return None, err
        
        vehicle_makes_airbags_df = primary_person_df.filter((col("PRSN_TYPE_ID") == "DRIVER") & (col("PRSN_INJRY_SEV_ID") == "KILLED") & (col("PRSN_AIRBAG_ID") == "NOT DEPLOYED")) \
            .join(units_df.select("CRASH_ID", "UNIT_NBR", "VEH_MAKE_ID").distinct(),on=["CRASH_ID", "UNIT_NBR"],how="inner") \
            .groupBy("VEH_MAKE_ID").agg(count("*").alias("count")) \
            .withColumn("RANK", rank().over(Window.orderBy(col("count").desc()))) \
            .filter(col("RANK") <= 5).select("RANK","VEH_MAKE_ID")

        return vehicle_makes_airbags_df, None

    def count_vehicles_valid_license_hit_and_run(self):
        """
        Count the number of vehicles involved in hit-and-run incidents with valid licenses.

        Returns:
            DataFrame or None: The DataFrame with the count of hit-and-run vehicles with valid licenses, or None if an error occurs.
            str or None: An error message if DataFrame retrieval fails, else None.
        """
        units_df, err = self.__df_store.get_df("units")
        if err:
                return None, err
        
        primary_person_df, err = self.__df_store.get_df("primary_person")
        if err:
                return None, err
        
        count_hnr_vehicles_df = units_df.join(primary_person_df, on=["CRASH_ID", "UNIT_NBR"], how="inner") \
        .filter((col("VEH_HNR_FL") == "Y") & (~col("DRVR_LIC_CLS_ID").isin("UNKNOWN", "UNLICENSED", "NA"))) \
        .agg(countDistinct(col("CRASH_ID"), col("UNIT_NBR")).alias("HIT_AND_RUN_VEHICLE_COUNT"))

        return count_hnr_vehicles_df, None

    def state_highest_accidents_no_female_involved(self):
        """
        Determine the state with the highest number of accidents where no females were involved.

        Returns:
            DataFrame or None: The DataFrame with the state having the most crashes with no females involved, or None if an error occurs.
            str or None: An error message if DataFrame retrieval fails, else None.
        """
        units_df, err = self.__df_store.get_df("units")
        if err:
                return None, err
        
        primary_person_df, err = self.__df_store.get_df("primary_person")
        if err:
                return None, err
        
        state_max_acc_df = primary_person_df.groupBy("CRASH_ID") \
        .agg(collect_set("PRSN_GNDR_ID").alias("gender_set")) \
        .filter(~array_contains("gender_set", "FEMALE")) \
        .select("CRASH_ID") \
        .join(units_df, "CRASH_ID", "left") \
        .groupBy("VEH_LIC_STATE_ID") \
        .agg(countDistinct("CRASH_ID").alias("cnt")) \
        .orderBy("cnt", ascending=False) \
        .limit(1) \
        .select(col("VEH_LIC_STATE_ID").alias("STATE_WITH_MOST_CRASHES_NO_FEMALE_INVOLVED"))

        return state_max_acc_df, None

    def most_fatal_veh_makes(self):
        """
        Determine the top 3rd to 5th most fatal vehicle makes that contribute to a largest number of injuries including death.

        Returns:
            DataFrame or None: The DataFrame with the top 3rd to 5th most fatal vehicle makes, or None if an error occurs.
            str or None: An error message if DataFrame retrieval fails, else None.
        """
        units_df, err = self.__df_store.get_df("units")
        if err:
                return None, err
        
        fatal_make_ids_df = units_df.select("CRASH_ID", "UNIT_NBR", "VEH_MAKE_ID", "TOT_INJRY_CNT", "DEATH_CNT").distinct() \
        .withColumn("TOT_CASLTY_CNT", col("TOT_INJRY_CNT") + col("DEATH_CNT")) \
        .groupBy("VEH_MAKE_ID") \
        .sum("TOT_CASLTY_CNT") \
        .orderBy(col("sum(TOT_CASLTY_CNT)").desc()) 

        top_third_to_fifth_makes_df = fatal_make_ids_df \
        .limit(5).subtract(fatal_make_ids_df.limit(2)) \
        .select("VEH_MAKE_ID") 

        return top_third_to_fifth_makes_df, None

    def top_ethnic_group_per_style(self):
        """
        Determine the top ethnic group for each vehicle body style involved in crashes.

        Returns:
            DataFrame or None: The DataFrame with the top ethnic group for each vehicle body style, or None if an error occurs.
            str or None: An error message if DataFrame retrieval fails, else None.
        """
        units_df, err = self.__df_store.get_df("units")
        if err:
                return None, err
        
        primary_person_df, err = self.__df_store.get_df("primary_person")
        if err:
                return None, err
        
        top_ethnic_user_group_df = units_df.join(primary_person_df, ["CRASH_ID", "UNIT_NBR"], "inner") \
        .groupBy("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID") \
        .agg(count("*").alias("count")) \
        .withColumn("top_ethnic_user_group", rank().over(Window.partitionBy("VEH_BODY_STYL_ID").orderBy(col("count").desc()))) \
        .filter(col("top_ethnic_user_group") == 1) \
        .select("VEH_BODY_STYL_ID", "PRSN_ETHNICITY_ID")

        return top_ethnic_user_group_df, None

    def top_zip_codes_alcohol_crashes(self):
        """
        Determine the top 5 zip codes with the highest number of alcohol-related crashes.

        Returns:
            DataFrame or None: The DataFrame with the top 5 zip codes and their crash counts, or None if an error occurs.
            str or None: An error message if DataFrame retrieval fails, else None.
        """
        primary_person_df, err = self.__df_store.get_df("primary_person")
        if err:
                return None, err
        
        alcoholic_crash_df = primary_person_df.filter((col("PRSN_ALC_RSLT_ID") == "Positive") & (col("DRVR_ZIP").isNotNull())) \
        .groupBy("DRVR_ZIP").agg(count("*").alias("crash_count")) \
        .withColumn("RANK_OF_HIGHEST_CRASHES_DUE_TO_ALCOHOL", rank().over(Window.orderBy(col("crash_count").desc()))) \
        .filter(col("RANK_OF_HIGHEST_CRASHES_DUE_TO_ALCOHOL") <= 5) \
        .select("RANK_OF_HIGHEST_CRASHES_DUE_TO_ALCOHOL", "DRVR_ZIP")

        return alcoholic_crash_df, None

    def count_distinct_crash_ids_high_damage(self):
        """
        Count the number of distinct crashes with high vehicle damage (VEH_DMAG_SCL~) is above 4 and car avails Insurance.

        Returns:
            DataFrame or None: The DataFrame with the count of crashes with high vehicle damage,
                            or None if an error occurs.
            str or None: An error message if DataFrame retrieval fails, else None.
        """
        units_df, err = self.__df_store.get_df("units")
        if err:
                return None, err
        
        damages_df, err = self.__df_store.get_df("damages")
        if err:
                return None, err
        
        count_high_damage_crashes_df = units_df.join(damages_df, ["CRASH_ID"], "left_anti") \
        .filter(units_df["FIN_RESP_TYPE_ID"].rlike("INSURANCE")) \
        .select(units_df["CRASH_ID"].alias("CRASH_ID"), "VEH_DMAG_SCL_1_ID", "VEH_DMAG_SCL_2_ID") \
        .withColumn("VEH_DMAG_SCL_1_ID", regexp_extract("VEH_DMAG_SCL_1_ID", "(\\d{1})", 1).cast("int")) \
        .withColumn("VEH_DMAG_SCL_2_ID", regexp_extract("VEH_DMAG_SCL_2_ID", "(\\d{1})", 1).cast("int")) \
        .where((col("VEH_DMAG_SCL_1_ID") > 4) | (col("VEH_DMAG_SCL_2_ID") > 4)) \
        .select(countDistinct("CRASH_ID").alias("COUNT_OF_CRASHES"))

        return count_high_damage_crashes_df, None

    def rank_veh_makes_with_speeding_offences(self):
        """
        Rank vehicle makes based on the number of speeding offences in the top states and colors.

        Returns:
            DataFrame or None: The DataFrame with the ranked vehicle makes,
                            or None if an error occurs.
            str or None: An error message if DataFrame retrieval fails, else None.
        """
        units_df, err = self.__df_store.get_df("units")
        if err:
                return None, err
        
        primary_person_df, err = self.__df_store.get_df("primary_person")
        if err:
                return None, err
        
        charges_df, err = self.__df_store.get_df("charges")
        if err:
                return None, err

        highest_offences_states_df = units_df.join(charges_df, ["CRASH_ID", "UNIT_NBR"], "inner") \
        .groupBy("VEH_LIC_STATE_ID") \
        .agg(count("*").alias("offence_count")) \
        .withColumn("rnk", rank().over(Window.orderBy(col("offence_count").desc()))) \
        .filter("rnk <= 25") \
        .select("VEH_LIC_STATE_ID")
        
        highest_offences_states_list = [ row[0] for row in highest_offences_states_df.collect() ]
        
        top_used_colours_df = units_df.select("CRASH_ID", "UNIT_NBR", "VEH_COLOR_ID").distinct() \
        .groupBy("VEH_COLOR_ID") \
        .agg(count("*").alias("count")) \
        .withColumn("rnk", rank().over(Window.orderBy(col("count").desc()))) \
        .filter("rnk <= 10") \
        .select("VEH_COLOR_ID")
        
        top_used_colours_list = [ row[0] for row in top_used_colours_df.collect() ]
        
        rank_vehicle_makes_df = primary_person_df.join(units_df, ["CRASH_ID", "UNIT_NBR"], "inner") \
        .join(charges_df, ["CRASH_ID", "UNIT_NBR"], "inner") \
        .filter((col("VEH_COLOR_ID").isin(top_used_colours_list)) &
                (col("VEH_LIC_STATE_ID").isin(highest_offences_states_list)) &
                (col("charge").rlike("SPEED")) &
                (~col("DRVR_LIC_CLS_ID").isin("UNKNOWN", "UNLICENSED", "NA"))) \
        .groupBy("VEH_MAKE_ID") \
        .agg(count("*").alias("make_count")) \
        .withColumn("RANK", rank().over(Window.orderBy(col("make_count").desc()))) \
        .select("RANK", "VEH_MAKE_ID").filter(col("RANK") <=5)
        
        return rank_vehicle_makes_df, None