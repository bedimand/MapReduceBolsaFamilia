import os
import glob
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def optimize_numeric_dtypes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Reduz o uso de memória ajustando tipos de dados numéricos:
    - Downcast de inteiros (signed ou unsigned conforme necessário)
    - Downcast de floats (para float32, se couber)
    """
    for col in df.select_dtypes(include=["int64", "Int64"]).columns:
        # se todos os valores são >= 0, faz downcast unsigned; senão signed
        if (df[col] >= 0).all():
            df[col] = pd.to_numeric(df[col], downcast="unsigned")
        else:
            df[col] = pd.to_numeric(df[col], downcast="signed")

    for col in df.select_dtypes(include=["float64"]).columns:
        df[col] = pd.to_numeric(df[col], downcast="float")

    return df

def csv_to_parquet(
    input_path: str,
    output_path: str,
    compression: str = "snappy",
    chunk_size: int = 500_000,
    delimiter: str = ";",
    encoding: str = "latin-1"
):
    """
    Lê o CSV em chunks, faz otimizações de dtypes numéricos
    e escreve em um único arquivo Parquet.
    """
    writer = None

    for chunk in pd.read_csv(
        input_path,
        sep=delimiter,
        encoding=encoding,
        chunksize=chunk_size,
        low_memory=False  # garante inferência estável de tipos
    ):
        # Ajusta dtypes numéricos
        chunk = optimize_numeric_dtypes(chunk)

        # Converte para tabela Arrow
        table = pa.Table.from_pandas(chunk, preserve_index=False)

        if writer is None:
            # Cria o writer com o schema do primeiro chunk
            writer = pq.ParquetWriter(
                output_path,
                table.schema,
                compression=compression
            )

        writer.write_table(table)

    if writer:
        writer.close()
        print(f"✅ Conversão concluída: {output_path}")
    else:
        print(f"⚠️  Nenhum dado lido de {input_path}; sem saída gerada.")

def main():
    # garante que a pasta de saída exista
    os.makedirs("parquet", exist_ok=True)

    # localiza todos os CSVs em data/*.csv
    csv_files = glob.glob(os.path.join("data", "*.csv"))

    if not csv_files:
        print("❌ Não foram encontrados arquivos CSV na pasta 'data/'.")
        return

    # processa cada CSV
    for csv_file in csv_files:
        base = os.path.splitext(os.path.basename(csv_file))[0]
        parquet_file = os.path.join("parquet", f"{base}.parquet")
        print(f"📥 Convertendo {csv_file} → {parquet_file}")
        csv_to_parquet(csv_file, parquet_file)

if __name__ == "__main__":
    main()
