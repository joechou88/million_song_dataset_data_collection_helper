import pandas as pd
import numpy as np
from sklearn.preprocessing import RobustScaler
from sklearn.ensemble import IsolationForest
from sklearn.linear_model import ElasticNet, Ridge

class Preprocess:
    def __init__(self, config):
        self.config = config
        self.target_col = 'artist_hotttnesss'
        self.exclude_cols = ['track_7digitalid', 'year', self.target_col]

    def remove_missing_values(self, df, input_path):
        print(f"--- Start removing missing values for {input_path} ---")

        df_clean = df.dropna()

        output_path = input_path.replace(".csv", "_remove_missing_values.csv")
        df_clean.to_csv(output_path, index=False)

        print(
            f"Rows before: {len(df)}, after: {len(df_clean)}\n"
            f"--- Complete removing missing values. Saved as {output_path} ---\n"
        )
        return df_clean, output_path

    def encode_categorical_variables(self, df, input_path):
        print("--- Start One-hot Encoding for categorical variables (term) ---")

        term_dummies = df['term'].str.get_dummies(sep=', ')
        df_encoded = pd.concat([df.drop('term', axis=1), term_dummies], axis=1)
        
        output_path = input_path.replace(".csv", "_encoded.csv")
        df_encoded.to_csv(output_path, index=False)
        print(
            f"Rows before: {len(df)}, after: {len(df_encoded)}\n"
            f"--- Complete One-hot Encoding. New feature count: {df_encoded.shape[1]} ---\n"
        )
        return df_encoded, output_path

    def remove_outliers(self, df, input_path):
        print("--- Start Outlier Detection (Isolation Forest) ---")

        numeric_cols = [c for c in df.select_dtypes(include=[np.number]).columns 
                        if c not in self.exclude_cols]
            
        iso_forest = IsolationForest(contamination=0.05, random_state=42, n_jobs=-1)
        outliers = iso_forest.fit_predict(df[numeric_cols])
        df_remove_outliers = df[outliers == 1]
        
        output_path = input_path.replace(".csv", "_remove_outliers.csv")
        df_remove_outliers.to_csv(output_path, index=False)
        print(
            f"Rows before: {len(df)}, after: {len(df_remove_outliers)}\n"
            f"--- Outliers removed ---\n"
        )
        return df_remove_outliers, output_path

    def scale_continuous_variables(self, df, input_path):
        print("--- Start Scaling for continuous variables (Robust Scaler) ---")
        
        numeric_cols = [c for c in df.select_dtypes(include=[np.number]).columns 
                        if c not in self.exclude_cols and df[c].nunique() > 2]
            
        scaler = RobustScaler()
        df[numeric_cols] = scaler.fit_transform(df[numeric_cols])
        
        output_path = input_path.replace(".csv", "_scaled.csv")
        df.to_csv(output_path, index=False)
        print("--- Complete Feature scaling with Robust Scaler. ---\n")
        return df, output_path

    def adaptive_elastic_net(self, df, input_path):
        print("--- Start Adaptive Elastic Net to mitigate multicollinearity problem ---")

        X = df.select_dtypes(include=[np.number]).drop(columns=self.exclude_cols)
        y = df[self.target_col]
        initial_feature_count = X.shape[1]
        
        ridge = Ridge(alpha=1.0)
        ridge.fit(X, y)
        weights = 1.0 / (np.abs(ridge.coef_) + 1e-5)
        X_weighted = X / weights
        
        model = ElasticNet(alpha=0.1, l1_ratio=0.5, random_state=42)
        model.fit(X_weighted, y)

        selected_features = X.columns[model.coef_ != 0].tolist()

        final_cols = [c for c in df.columns if c in selected_features or c in self.exclude_cols or not np.issubdtype(df[c].dtype, np.number)]
        df_final = df[final_cols]

        output_path = input_path.replace(".csv", "_adaptive_elastic_net_selected.csv")
        df_final.to_csv(output_path, index=False)
        
        print(
            f"Columns before: {initial_feature_count}, after: {len(selected_features)}\n"
            f"--- Complete Adaptive Elastic Net. Final file saved as {output_path} ---\n"
        )
        
        return df_final, output_path
