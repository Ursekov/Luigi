# [Применение Luigi для создания пайплайна скачивания и обработки данных](https://github.com/Ursekov/Luigi)

## Оглавление  
[1. Описание проекта](https://github.com/Ursekov/Luigi?tab=readme-ov-file#%D0%BE%D0%BF%D0%B8%D1%81%D0%B0%D0%BD%D0%B8%D0%B5-%D0%BF%D1%80%D0%BE%D0%B5%D0%BA%D1%82%D0%B0)  
[2. Краткая информация о данных](https://github.com/Ursekov/Luigi?tab=readme-ov-file#%D0%BA%D1%80%D0%B0%D1%82%D0%BA%D0%B0%D1%8F-%D0%B8%D0%BD%D1%84%D0%BE%D1%80%D0%BC%D0%B0%D1%86%D0%B8%D1%8F-%D0%BE-%D0%B4%D0%B0%D0%BD%D0%BD%D1%8B%D1%85)  
[3. Этапы работы над проектом](https://github.com/Ursekov/Luigi?tab=readme-ov-file#%D1%8D%D1%82%D0%B0%D0%BF%D1%8B-%D1%80%D0%B0%D0%B1%D0%BE%D1%82%D1%8B-%D0%BD%D0%B0%D0%B4-%D0%BF%D1%80%D0%BE%D0%B5%D0%BA%D1%82%D0%BE%D0%BC)  
[4. Результаты](https://github.com/Ursekov/Luigi?tab=readme-ov-file#%D1%80%D0%B5%D0%B7%D1%83%D0%BB%D1%8C%D1%82%D0%B0%D1%82%D1%8B)    


### Описание проекта  
Целью проекта является построение пайплана загрузки и обработки  данных при помощи модуля [Luigi](https://luigi.readthedocs.io/en/stable/index.html)


Код находится в файле с расширением .py под названием [Luigi Pipeline](https://github.com/Ursekov/Luigi/blob/master/Luigi%20Pipeline.py)

Список используемых пакетов среды находится в файле [requirements.txt](https://github.com/Ursekov/Luigi/blob/master/requirements.txt)

Данный код предназначен для запуска в Unix подобных системах.

:arrow_up:[к оглавлению](https://github.com/Ursekov/Luigi?tab=readme-ov-file#%D0%BE%D0%B3%D0%BB%D0%B0%D0%B2%D0%BB%D0%B5%D0%BD%D0%B8%D0%B5)


### Краткая информация о данных
Данные скачиваются со следующего [сайта](https://www.ncbi.nlm.nih.gov/geo/query/acc.cgi?acc=GSE68849).

Обработанные файлы лежат в облаке на [Google Диск](https://drive.google.com/drive/folders/1Tz-4Q4HIthCfgDn5sYoBDsYU5fefkhpr?usp=sharing).
  
:arrow_up:[к оглавлению](https://github.com/Ursekov/Luigi?tab=readme-ov-file#%D0%BE%D0%B3%D0%BB%D0%B0%D0%B2%D0%BB%D0%B5%D0%BD%D0%B8%D0%B5)


### Этапы работы над проектом  
1. Загрузка файла;
2. Разарзивирование и сохранение отдельных файлов;
3. Обработка файлов;
4. Удаление ненужных файлов;

:arrow_up:[к оглавлению](https://github.com/Ursekov/Luigi?tab=readme-ov-file#%D0%BE%D0%B3%D0%BB%D0%B0%D0%B2%D0%BB%D0%B5%D0%BD%D0%B8%D0%B5)


### Результаты:  
Файлы скачаны, разархивированы и обработаны.

Можно использовать следующую команду в терминале для запуска всего пайплайна:
"python3 -m Luigi\ Pipeline DelFiles --local-scheduler"

Чтобы запустить определнную задачу, необходимо заменить DelFiles на существующую задачу.

:arrow_up:[к оглавлению](https://github.com/Ursekov/Luigi?tab=readme-ov-file#%D0%BE%D0%B3%D0%BB%D0%B0%D0%B2%D0%BB%D0%B5%D0%BD%D0%B8%D0%B5)



Если информация по этому проекту покажется вам интересной или полезной, то я буду очень вам благодарен, если отметите репозиторий и профиль ⭐️⭐️⭐️-дами