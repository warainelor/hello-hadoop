# Запуск демонов Hadoop

## Запустим NameNode и DataNode

```bash
start-dfs.sh
```

## Запустим YARN

```bash
start-yarn.sh
```

## Проверим работающие процессы

```bash
jps
```

## Пример вывода
```bash
1234 NameNode  
5678 DataNode  
9101 ResourceManager  
1122 NodeManager  
```

# Проверка веб-интерфейсов


HDFS NameNode UI: http://localhost:9870
YARN ResourceManager UI: http://localhost:8088