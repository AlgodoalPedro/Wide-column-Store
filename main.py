from cassandra.cluster import Cluster

# Conecta com cassandra
cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect()

# Cria keyspace
session.execute(f"""
    CREATE KEYSPACE IF NOT EXISTS banco_projeto_2
    WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': '1' }};
""")
session.set_keyspace("banco_projeto_2")


# Criar tabelas
def create_tables():
    # query = f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'banco_projeto_2';"
    # rows = session.execute(query)
    # tables = [row.table_name for row in rows]
    #
    # # Deletar todas as tabelas
    # for table in tables:
    #     try:
    #         session.execute(f"DROP TABLE IF EXISTS {table};")
    #         print(f"Tabela '{table}' deletada com sucesso.")
    #     except Exception as e:
    #         print(f"Erro ao deletar tabela '{table}': {e}")

    session.execute("""
        CREATE TABLE IF NOT EXISTS alunos (
            matricula TEXT PRIMARY KEY,
            nome TEXT,
            curso_id TEXT,
            ano_ingresso INT,
            status TEXT,
            tcc_id TEXT
        );
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS historico_escolar (
            matricula TEXT,
            ano INT,
            semestre TEXT,
            disciplina_id TEXT,
            nota_final FLOAT,
            nome_disciplina TEXT,
            PRIMARY KEY (matricula, ano, semestre, disciplina_id)
        );
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS professores (
            id_professor TEXT PRIMARY KEY,
            nome TEXT,
            nome_departamento TEXT,
            chefe_departamento BOOLEAN
        );
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS historico_professor (
            id_professor TEXT,
            ano INT,
            semestre TEXT,
            disciplina_id TEXT,
            nome_disciplina TEXT,
            PRIMARY KEY (id_professor, ano, semestre, disciplina_id)
        );
    """)

    session.execute("""
        CREATE TABLE IF NOT EXISTS tccs (
            tcc_id TEXT PRIMARY KEY,
            titulo TEXT,
            orientador_id TEXT,
            alunos_ids LIST<TEXT>
        );
    """)


# Popular as tabelas
def populate_data():
    # Inserir alunos
    session.execute("""
        INSERT INTO alunos (matricula, nome, curso_id, ano_ingresso, status, tcc_id)
        VALUES ('22122111', 'Pedro Henrique', '1', 2021, 'Ativo', '1');
    """)
    session.execute("""
        INSERT INTO alunos (matricula, nome, curso_id, ano_ingresso, status, tcc_id)
        VALUES ('22122222', 'João Silva', '1', 2020, 'Formado', '2');
    """)

    # Inserir histórico escolar
    session.execute("""
        INSERT INTO historico_escolar (matricula, ano, semestre, disciplina_id, nota_final, nome_disciplina)
        VALUES ('22122111', 2024, '6', 'CC6252', 10, 'Compiladores');
    """)
    session.execute("""
        INSERT INTO historico_escolar (matricula, ano, semestre, disciplina_id, nota_final, nome_disciplina)
        VALUES ('22122222', 2023, '6', 'CC6252', 2, 'Banco de Dados Avançado');
    """)


    # Inserir professores
    session.execute("""
        INSERT INTO professores (id_professor, nome, nome_departamento, chefe_departamento)
        VALUES ('1', 'Prof. Charles', 'Ciência da Computação', true);
    """)
    session.execute("""
        INSERT INTO professores (id_professor, nome, nome_departamento, chefe_departamento)
        VALUES ('2', 'Prof. Leonardo', 'Ciência da Computação', false);
    """)

    # Inserir histórico professor
    session.execute("""
        INSERT INTO historico_professor (id_professor, ano, semestre, disciplina_id, nome_disciplina)
        VALUES ('1', 2024, '1', 'CC6252', 'Compiladores');
    """)
    session.execute("""
        INSERT INTO historico_professor (id_professor, ano, semestre, disciplina_id, nome_disciplina)
        VALUES ('2', 2024, '1', 'CC6240', 'Banco de Dados Avançado');
    """)

    # Inserir TCCs
    session.execute("""
        INSERT INTO tccs (tcc_id, titulo, orientador_id, alunos_ids)
        VALUES ('1', 'Compiladores Quânticos', '1', ['22122111']);
    """)
    session.execute("""
        INSERT INTO tccs (tcc_id, titulo, orientador_id, alunos_ids)
        VALUES ('2', 'Banco de Dados Magnéticos', '2', ['22122222']);
    """)


# Consultas para relatórios
def query_reports():
    # Histórico do aluno
    print("\nHistórico Escolar:")
    rows = session.execute("""
        SELECT disciplina_id, nome_disciplina, semestre, ano, nota_final
        FROM historico_escolar
        WHERE matricula='22122111';
    """)
    for row in rows:
        print(row)

    # Histórico de disciplinas ministradas por um Prof.
    print("\nHistórico do Professor:")
    rows = session.execute("""
        SELECT nome_disciplina, semestre, ano
        FROM historico_professor
        WHERE id_professor='2';
    """)
    for row in rows:
        print(row)

    # Alunos formado
    print("\nAlunos Formados:")
    rows = session.execute("""
        SELECT matricula, nome
        FROM alunos
        WHERE status='Formado'
        ALLOW FILTERING;
    """)
    for row in rows:
        print(row)

    # Prof. chefes de departamento
    print("\nChefes de Departamento:")
    rows = session.execute("""
        SELECT nome, nome_departamento
        FROM professores
        WHERE chefe_departamento=true
        ALLOW FILTERING;
    """)
    for row in rows:
        print(row)

    # Alunos e orientadores de TCC
    print("\nGrupos de TCC:")
    rows = session.execute("""
        SELECT titulo, orientador_id, alunos_ids
        FROM tccs
        ALLOW FILTERING;
    """)
    for row in rows:
        print(row)


# Chamada das funções
create_tables()
populate_data()
query_reports()

cluster.shutdown()