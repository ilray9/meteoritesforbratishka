import pandas as pd
import numpy as np

# Загрузка данных
df = pd.read_csv('meteorite-landings.csv')

# Предварительный просмотр данных
print("Первые 5 строк данных:")
print(df.head())
print("\n" + "="*50 + "\n")

# Информация о данных
print("Информация о датасете:")
print(df.info())
print("\n" + "="*50 + "\n")

# Очистка данных - удаление строк с пропущенными значениями в ключевых столбцах
df_clean = df.dropna(subset=['mass', 'reclat', 'reclong'])

# 1. Средняя масса метеорита
average_mass = df_clean['mass'].mean()
print(f"1. Средняя масса метеорита: {average_mass:.2f} грамм")
print(f"   ({average_mass/1000:.2f} кг)")
print("\n" + "="*50 + "\n")

# 2. Среднестатистическая точка падения по координатам
mean_lat = df_clean['reclat'].mean()
mean_long = df_clean['reclong'].mean()
print(f"2. Среднестатистическая точка падения:")
print(f"   Средняя широта: {mean_lat:.6f}°")
print(f"   Средняя долгота: {mean_long:.6f}°")
print("\n" + "="*50 + "\n")

# 3. Статистика по процентному соотношению классов метеоритов
# Проверяем наличие столбца с классами (может называться 'recclass' или 'class')
class_column = 'recclass' if 'recclass' in df.columns else 'class'

# Группировка по классам метеоритов
class_stats = df[class_column].value_counts()

# Рассчитываем проценты
class_percentages = (class_stats / len(df) * 100).round(2)

print("3. Статистика по классам метеоритов:")
print(f"   Всего уникальных классов: {len(class_stats)}")
print("\n   Топ-10 самых распространенных классов:")
for i, (class_name, percentage) in enumerate(class_percentages.head(10).items(), 1):
    print(f"   {i:2}. {class_name:20} - {percentage:5.2f}% ({class_stats[class_name]:,} шт.)")

print(f"\n   Топ-10 самых редких классов:")
for i, (class_name, percentage) in enumerate(class_percentages.tail(10).items(), 1):
    print(f"   {i:2}. {class_name:20} - {percentage:5.2f}% ({class_stats[class_name]:,} шт.)")

print("\n" + "="*50 + "\n")

# Дополнительная статистика
print("4. Дополнительная статистика:")
print(f"   Всего записей в датасете: {len(df):,}")
print(f"   Записей после очистки (с массой и координатами): {len(df_clean):,}")

# Статистика по массам
print(f"\n   Статистика по массам метеоритов:")
print(f"   Минимальная масса: {df_clean['mass'].min():.2f} г")
print(f"   Максимальная масса: {df_clean['mass'].max():.2f} г")
print(f"   Медианная масса: {df_clean['mass'].median():.2f} г")
print(f"   Стандартное отклонение: {df_clean['mass'].std():.2f} г")

# Визуализация распределения координат
print(f"\n   Географическое распределение:")
print(f"   Минимальная широта: {df_clean['reclat'].min():.2f}°")
print(f"   Максимальная широта: {df_clean['reclat'].max():.2f}°")
print(f"   Минимальная долгота: {df_clean['reclong'].min():.2f}°")
print(f"   Максимальная долгота: {df_clean['reclong'].max():.2f}°")

# Сохранение результатов в CSV
print("\n" + "="*50 + "\n")
print("5. Сохранение результатов анализа...")

# Сохраняем статистику по классам
class_stats_df = pd.DataFrame({
    'class': class_percentages.index,
    'count': class_stats.values,
    'percentage': class_percentages.values
})
class_stats_df.to_csv('meteorite_class_statistics.csv', index=False)

# Сохраняем общую статистику
summary_stats = pd.DataFrame({
    'metric': ['average_mass_grams', 'average_mass_kg', 'mean_latitude', 'mean_longitude', 
               'total_records', 'cleaned_records', 'unique_classes'],
    'value': [average_mass, average_mass/1000, mean_lat, mean_long, 
              len(df), len(df_clean), len(class_stats)]
})
summary_stats.to_csv('meteorite_summary_statistics.csv', index=False)

print("   Результаты сохранены в файлы:")
print("   - meteorite_class_statistics.csv")
print("   - meteorite_summary_statistics.csv")

# Дополнительный анализ: распределение по годам и типам падения
if 'year' in df.columns and 'fall' in df.columns:
    # Конвертируем год в числовой формат
    df['year'] = pd.to_numeric(df['year'], errors='coerce')
    
    print("\n" + "="*50 + "\n")
    print("6. Дополнительный анализ:")
    
    # Анализ по типам падения (Fell vs Found)
    fall_stats = df['fall'].value_counts()
    fall_percentages = (fall_stats / len(df) * 100).round(2)
    
    print("\n   Распределение по типам обнаружения:")
    for fall_type, count in fall_stats.items():
        percentage = fall_percentages[fall_type]
        print(f"   {fall_type}: {count:,} ({percentage}%)")
    
    # Анализ по годам
    print(f"\n   Статистика по годам падения:")
    print(f"   Самый ранний год: {int(df['year'].min())}")
    print(f"   Самый поздний год: {int(df['year'].max())}")
    print(f"   Средний год: {df['year'].mean():.0f}")
    
    # Анализ по десятилетиям
    df['decade'] = (df['year'] // 10) * 10
    decade_stats = df['decade'].value_counts().sort_index()
    
    print(f"\n   Количество метеоритов по десятилетиям:")
    for decade, count in decade_stats.head(10).items():
        if not pd.isna(decade):
            print(f"   {int(decade)}-е: {count:,} метеоритов")

# Анализ самых тяжелых метеоритов
print("\n" + "="*50 + "\n")
print("7. Топ-5 самых тяжелых метеоритов:")

heavy_meteorites = df_clean.nlargest(5, 'mass')[['name', 'mass', 'reclat', 'reclong', class_column]]
for i, (_, row) in enumerate(heavy_meteorites.iterrows(), 1):
    print(f"   {i}. {row['name']}")
    print(f"      Масса: {row['mass']:,.0f} г ({row['mass']/1000:,.1f} кг)")
    print(f"      Класс: {row[class_column]}")
    print(f"      Координаты: {row['reclat']:.2f}°, {row['reclong']:.2f}°")
