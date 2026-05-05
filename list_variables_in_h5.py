import h5py

file_path = 'msd_summary_file.h5'

with h5py.File(file_path, 'r') as h5:
    groups = ['analysis', 'metadata', 'musicbrainz']
    
    print("=== Million Song Dataset 所有變數清單 ===")
    
    all_variables_count = 0
    
    for group in groups:
        fields = h5[group]['songs'].dtype.names
        num_fields = len(fields)
        all_variables_count += num_fields
        
        print(f"\n📌 群組: {group} (共 {num_fields} 個變數)")
        print("-" * 50)

        for i in range(0, num_fields, 3):
            chunk = fields[i:i+3]
            print(" | ".join(f"{name:<25}" for name in chunk))
            
    print("\n" + "=" * 40)
    print(f"總計變數數量: {all_variables_count}")
    print(f"總計歌曲數量: {h5['metadata']['songs'].shape[0]:,}")
