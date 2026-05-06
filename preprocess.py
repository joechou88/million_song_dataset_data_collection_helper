import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix
from sklearn.preprocessing import RobustScaler
from sklearn.ensemble import IsolationForest
from sklearn.ensemble import ExtraTreesRegressor
from sklearn.linear_model import ElasticNet, Ridge
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import IterativeImputer
from tqdm import tqdm

class Preprocess:
    def __init__(self, config):
        self.config = config
        self.target_col = 'artist_hotttnesss'
        self.exclude_cols = ['track_7digitalid', 'year', self.target_col]

    def missing_values_imputation(self, df):
        numeric_columns = [column for column in df.select_dtypes(include=[np.number]).columns 
                           if column not in self.exclude_cols]
        rows_with_nan = df[numeric_columns].isna().any(axis=1).sum()
        if rows_with_nan == 0:
            print("No missing values detected.")
            return df

        columns_to_impute = df[numeric_columns].columns[df[numeric_columns].isna().any()].tolist()
        max_iteration = 5
        total_steps = len(columns_to_impute) * max_iteration
        pbar = tqdm(total=total_steps, desc="MICE Process")
        class TqdmEstimator(ExtraTreesRegressor):
            def fit(self, X, y, **kwargs):
                res = super().fit(X, y, **kwargs)
                pbar.update(1)
                return res
        multiple_imputation_model = IterativeImputer(
            estimator=TqdmEstimator(n_estimators=10, n_jobs=-1, random_state=42),
            max_iter=max_iteration,
            n_nearest_features=20, 
            random_state=42,
            verbose=0
        )
        try:
            imputed_numeric_matrix = multiple_imputation_model.fit_transform(df[numeric_columns])
        finally:
            pbar.close()

        df_clean = df.copy()
        df_clean[numeric_columns] = imputed_numeric_matrix
        print(f"Imuted missing values for {rows_with_nan} Observations (Total: {len(df)}")
        return df_clean

    def encode_categorical_variables(self, df):
        df['term'] = df['term'].astype(str)
        vectorizer = CountVectorizer(tokenizer=lambda x: x.split(', '), token_pattern=None, min_df=0.001)
        sparse_matrix = vectorizer.fit_transform(df['term'])     
        term_dummies = pd.DataFrame.sparse.from_spmatrix(
            sparse_matrix, 
            index=df.index, 
            columns=[f"term_{c}" for c in vectorizer.get_feature_names_out()]
        ).astype(pd.SparseDtype("int8", 0))
        df_encoded = pd.concat([df.drop('term', axis=1), term_dummies], axis=1)
        print(f"Rows before: {len(df)}, after: {len(df_encoded)}. New feature count: {df_encoded.shape[1]}")
        return df_encoded

    def remove_outliers(self, df):
        numeric_cols = [c for c in df.select_dtypes(include=[np.number]).columns 
                        if c not in self.exclude_cols]
        X_outlier = csr_matrix(df[numeric_cols].values)
        iso_forest = IsolationForest(contamination=0.05, random_state=42, n_jobs=-1)
        outliers = iso_forest.fit_predict(X_outlier)
        df_remove_outliers = df[outliers == 1]
        print(f"Rows before: {len(df)}, after: {len(df_remove_outliers)}")
        return df_remove_outliers

    def scale_continuous_variables(self, df):
        numeric_cols = [c for c in df.select_dtypes(include=[np.number]).columns 
                        if c not in self.exclude_cols and df[c].nunique() > 2]       
        scaler = RobustScaler()
        df[numeric_cols] = scaler.fit_transform(df[numeric_cols])
        return df

    def adaptive_elastic_net(self, df):
        X = df.select_dtypes(include=[np.number]).drop(columns=self.exclude_cols)
        y = df[self.target_col]
        initial_feature_count = X.shape[1]
        X_sparse = csr_matrix(X.values)
        
        ridge = Ridge(alpha=1.0)
        ridge.fit(X_sparse, y)
        weights = 1.0 / (np.abs(ridge.coef_) + 1e-5)
        X_weighted = X_sparse.multiply(1.0 / weights)
        
        model = ElasticNet(alpha=0.1, l1_ratio=0.5, random_state=42)
        model.fit(X_weighted, y)

        selected_features = X.columns[model.coef_ != 0].tolist()

        final_cols = []
        for c in df.columns:
            if c in selected_features or c in self.exclude_cols or not pd.api.types.is_numeric_dtype(df[c].dtype):
                final_cols.append(c)
        df_final = df[final_cols]
        print(f"Columns before: {initial_feature_count}, after: {len(selected_features)}")     
        return df_final
