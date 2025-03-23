package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Простая программа на Hadoop, которая подсчитывает количество вхождений каждого слова в тексте.
 */
public class HelloHadoop {

    /**
     * Mapper класс.
     * Принимает входные данные (строки текста) и выдает пары <слово, 1>.
     */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        // Константа для значения 1
        private final static IntWritable one = new IntWritable(1);
        // Переменная для хранения слова
        private Text word = new Text();

        /**
         * Метод map.
         * Вызывается для каждой строки входных данных.
         *
         * @param key     — ключ (обычно игнорируется).
         * @param value   — строка текста.
         * @param context — контекст для записи выходных данных.
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Разбиваем строку на слова с помощью StringTokenizer
            StringTokenizer itr = new StringTokenizer(value.toString());
            // Обрабатываем каждое слово
            while (itr.hasMoreTokens()) {
                // Устанавливаем слово в переменную word
                word.set(itr.nextToken());
                // Записываем пару <слово, 1> в контекст
                context.write(word, one);
            }
        }
    }

    /**
     * Reducer класс.
     * Принимает пары <слово, [1, 1, ...]> и суммирует количество вхождений.
     */
    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        // Переменная для хранения результата
        private IntWritable result = new IntWritable();

        /**
         * Метод reduce.
         * Вызывается для каждого уникального слова.
         *
         * @param key     — слово.
         * @param values  — список значений (всегда [1, 1, ...]).
         * @param context — контекст для записи выходных данных.
         */
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            // Суммируем все значения (1) для данного слова
            for (IntWritable val : values) {
                sum += val.get();
            }
            // Устанавливаем результат
            result.set(sum);
            // Записываем пару <слово, сумма> в контекст
            context.write(key, result);
        }
    }

    /**
     * Основной метод программы.
     *
     * @param args — аргументы командной строки:
     *             args[0] — путь к входному файлу,
     *             args[1] — путь к выходной директории.
     */
    public static void main(String[] args) throws Exception {
        // Создаем конфигурацию Hadoop
        Configuration conf = new Configuration();
        // Создаем задачу (Job) с именем "Hello Hadoop"
        Job job = Job.getInstance(conf, "Hello Hadoop");
        // Указываем класс, который содержит main метод
        job.setJarByClass(HelloHadoop.class);
        // Указываем класс Mapper
        job.setMapperClass(TokenizerMapper.class);
        // Указываем класс Combiner (опционально, для оптимизации)
        job.setCombinerClass(IntSumReducer.class);
        // Указываем класс Reducer
        job.setReducerClass(IntSumReducer.class);
        // Указываем тип выходного ключа (слово)
        job.setOutputKeyClass(Text.class);
        // Указываем тип выходного значения (количество)
        job.setOutputValueClass(IntWritable.class);

        // Указываем путь к входному файлу
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // Указываем путь к выходной директории
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Запускаем задачу и ждем ее завершения
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}