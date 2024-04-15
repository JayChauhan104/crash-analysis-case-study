from pyspark.sql import SparkSession
import argparse

from src.utils import load_config,df_write
from src.df_store import DFStore
from src.crash_analysis import CrashAnalysis

def main(config_file):
    """
    Main function to perform crash analysis.

    Reads configuration file, initializes Spark session, loads dataframes,
    performs crash analysis, writes output dataframes, and stops Spark session.

    Returns:
        None
    """
    try:
        # Load configuration
        config = load_config(config_file)

        # Initialize spark session
        spark = SparkSession.builder.appName("CrashAnalysisCaseStudy").getOrCreate()
        
        # Initialize dataframe store
        df_store = DFStore(spark_app=spark)

        # Load dataframes from CSV
        for df_name, source_path in config.get("data_sources").items():
            df_store.load_df_from_csv(source_path, df_name, header=True, inferSchema=True)

        # Initialize CrashAnalysis instance
        analysis = CrashAnalysis(df_store=df_store)

        # Perform crash analysis and handle errors
        num_crashes, err = analysis.analyse_fatal_male_crashes()
        if err:
            print(err)

        two_wheelers_booked, err = analysis.count_two_wheeler_crashes()
        if err:
            print(err)
        
        top_five_makes_no_airbags, err = analysis.top_vehicle_makes_driver_fatal_no_airbags()
        if err:
            print(err)
        
        num_veh_hnr, err = analysis.count_vehicles_valid_license_hit_and_run()
        if err:
            print(err)
        
        st_w_highest_num_acc_no_females, err = analysis.state_highest_accidents_no_female_involved()
        if err:
            print(err)
        
        third_fifth_fatal_makes, err = analysis.most_fatal_veh_makes()
        if err:
            print(err)
        
        top_ethnic_user, err = analysis.top_ethnic_group_per_style()
        if err:
            print(err)
        
        top_zip_cd_crash_alc, err = analysis.top_zip_codes_alcohol_crashes()
        if err:
            print(err)
        
        num_crashes_high_damage, err = analysis.count_distinct_crash_ids_high_damage()
        if err:
            print(err)
        
        top_five_makes_speeding, err = analysis.rank_veh_makes_with_speeding_offences()
        if err:
            print(err)
        
        # Prepare output dataframes
        output_dfs = {
            "output_1": num_crashes,
            "output_2": two_wheelers_booked,
            "output_3": top_five_makes_no_airbags,
            "output_4": num_veh_hnr,
            "output_5": st_w_highest_num_acc_no_females,
            "output_6": third_fifth_fatal_makes,
            "output_7": top_ethnic_user,
            "output_8": top_zip_cd_crash_alc,
            "output_9": num_crashes_high_damage,
            "output_10": top_five_makes_speeding
        }

        # Write output dataframes
        df_write(output_dfs, config)

    except Exception as e:
        print("Error occured during analysis:", e)

    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Crash Analysis")
    parser.add_argument("config_file", type=str, help="Path to the configuration file")
    args = parser.parse_args()
    main(args.config_file)