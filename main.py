import os
from rebuild_msd_database import MSDDatasetIntegrator
from manage_msd_db import MSDQueryTool

def main():
    integrator = MSDDatasetIntegrator()
    required_items = {
        "Million_Song_Dataset.csv": integrator.csv_path,
        "All_sample_properties.csv": integrator.property_path,
        "Million_Song_Dataset_Benchmarks": integrator.arff_dir,
        "SQLite_DB": integrator.side_db_dir
    }
    for name, path in required_items.items():
        if not os.path.isfile(path):
            print(f"[FileNotFoundError] Please input file/folder '{name}' at: {path}")
            return

    # 3. 初始化查詢工具
    print("正在初始化 MSDQueryTool ...")
    query_tool = MSDQueryTool(db_path=DB_PATH) 
    print("工具初始化成功！")

    # 4. 接下來就可以呼叫工具的內部方法進行各種查詢分析了
    # 例如: result = query_tool.some_query_function("某首歌的 track_id")
    # print(result)

if __name__ == "__main__":
    main()