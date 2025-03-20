from datetime import datetime, timedelta, timezone
import pickle
from utils import extract,round_off_timestamp,transform,load,feature_col_names








if __name__=="__main__":

    with open("../DE_LU_load_prediction_xgboost_model.pkl", "rb") as f:
        model = pickle.load(f)
        print("Model loaded")



    while True:

        current_time = datetime.now(timezone.utc)
        if current_time.minute in [1,16,31,46] and current_time.second==0:

            start_time = round_off_timestamp(current_time - timedelta(minutes=180))
            end_time = round_off_timestamp(current_time - timedelta(minutes=90))
            print('start:',start_time)
            print('end:',end_time)

            extracted_df = extract(start_time, end_time)
            print('len:', len(extracted_df))
            print(extracted_df.head())
            print(extracted_df.tail())

            transformed_df=transform(extracted_df)

            timestamp=transformed_df.loc[len(transformed_df)-1,['utc_timestamp']].iloc[0].strftime('%Y-%m-%d %H:%M:%S')
            load_actual_value = int(transformed_df.loc[len(transformed_df) - 1, ['DE_load_actual_entsoe_transparency']].iloc[0])
            feature_series= transformed_df.loc[len(transformed_df) - 1, feature_col_names]
            load_forecasted_value=int(model.predict([feature_series])[0])

            load(timestamp,load_forecasted_value,load_actual_value)




