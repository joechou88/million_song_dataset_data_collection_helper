import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix
from sklearn.preprocessing import RobustScaler
from sklearn.ensemble import IsolationForest
from sklearn.linear_model import ElasticNet, Ridge
from sklearn.feature_extraction.text import CountVectorizer

class Preprocess:
    def __init__(self, config):
        self.config = config
        self.target_col = 'artist_hotttnesss'
        self.exclude_cols = ['track_7digitalid', 'year', self.target_col]

    def remove_missing_values(self, df):
        df_clean = df.dropna()
        print(f"Rows before: {len(df)}, after: {len(df_clean)}")
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
