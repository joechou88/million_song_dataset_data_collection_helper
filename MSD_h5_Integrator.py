import h5py
import pandas as pd

class MSDH5Integrator:
    def __init__(self, config):
        self.config = config

    def integrate(self):
        columns_to_drop = ['genre', 'idx_artist_terms', 'idx_similar_artists']
        with h5py.File(self.config.h5_db_path, 'r') as h5:
            total_songs = h5['metadata']['songs'].shape[0]
            chunk_size = 100000
            print(f"Processing number of songs: {total_songs}")

            for start in range(0, total_songs, chunk_size):
                end = min(start + chunk_size, total_songs)

                df_a = pd.DataFrame(h5['analysis']['songs'][start:end])
                df_m = pd.DataFrame(h5['metadata']['songs'][start:end])
                df_mb = pd.DataFrame(h5['musicbrainz']['songs'][start:end])

                df_chunk = pd.concat([df_a, df_m, df_mb], axis=1)
                df_chunk.drop(columns=columns_to_drop, inplace=True, errors='ignore')
                
                str_cols = df_chunk.select_dtypes([object]).columns
                for col in str_cols:
                    df_chunk[col] = df_chunk[col].apply(lambda x: x.decode('utf-8') if isinstance(x, bytes) else x)

                if start == 0:
                    df_chunk.to_csv(self.config.h5_to_csv_path, index=False, mode='w', encoding='utf-8')
                else:
                    df_chunk.to_csv(self.config.h5_to_csv_path, index=False, mode='a', header=False, encoding='utf-8')
                    
                print(f"Process: {end} / {total_songs}")

        print(f"🎉 Convert .h5 to .csv. Saved as: {self.config.h5_to_csv_path}")