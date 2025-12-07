import pandas as pd
import numpy as np
import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message
from aiogram.utils.keyboard import ReplyKeyboardBuilder
import logging

# Загрузка данных
df = pd.read_csv('meteorite-landings.csv')
df_clean = df.dropna(subset=['mass', 'reclat', 'reclong'])
class_column = 'recclass' if 'recclass' in df.columns else 'class'

# Настройка бота
API_TOKEN = 'Paste_ur_bot_token'
bot = Bot(token=API_TOKEN)
dp = Dispatcher()
logging.basicConfig(level=logging.INFO)

# Создаем клавиатуру с командами
def get_main_keyboard():
    builder = ReplyKeyboardBuilder()
    commands = [
        "/start", "/preview", "/info", "/avg_mass", "/avg_coords",
        "/class_stats", "/additional_stats", "/heavy_meteorites",
        "/year_analysis", "/save_results", "/all_stats"
    ]
    for command in commands:
        builder.add(types.KeyboardButton(text=command))
    builder.adjust(2)  # 2 кнопки в строке
    return builder.as_markup(resize_keyboard=True)

# ==================== ФУНКЦИИ ДЛЯ КАЖДОГО ПУНКТА ====================

# Функция для предварительного просмотра данных
async def preview_data(message: Message):
    result = "🔍 Первые 5 строк данных:\n\n"
    result += df.head().to_string()
    await message.answer(result)

# Функция для информации о датасете
async def dataset_info(message: Message):
    import io
    buffer = io.StringIO()
    df.info(buf=buffer)
    info_str = buffer.getvalue()
    
    result = "📊 Информация о датасете:\n"
    result += f"Колонки: {list(df.columns)}\n"
    result += f"Всего строк: {len(df)}\n"
    result += f"Типы данных:\n{info_str}"
    await message.answer(result[:4000])

# Функция для средней массы метеорита
async def average_mass_stat(message: Message):
    average_mass = df_clean['mass'].mean()
    result = "⚖️ Средняя масса метеорита:\n\n"
    result += f"📊 {average_mass:.2f} грамм\n"
    result += f"📦 {average_mass/1000:.2f} кг"
    await message.answer(result)

# Функция для среднестатистической точки падения
async def average_coordinates(message: Message):
    mean_lat = df_clean['reclat'].mean()
    mean_long = df_clean['reclong'].mean()
    result = "📍 Среднестатистическая точка падения:\n\n"
    result += f"🌐 Средняя широта: {mean_lat:.6f}°\n"
    result += f"🌐 Средняя долгота: {mean_long:.6f}°"
    await message.answer(result)

# Функция для статистики по классам метеоритов
async def class_statistics(message: Message):
    class_stats = df[class_column].value_counts()
    class_percentages = (class_stats / len(df) * 100).round(2)
    
    result = "🎯 Статистика по классам метеоритов:\n\n"
    result += f"📈 Всего уникальных классов: {len(class_stats)}\n\n"
    result += "🏆 Топ-10 самых распространенных классов:\n\n"
    
    for i, (class_name, percentage) in enumerate(class_percentages.head(10).items(), 1):
        result += f"{i}. {class_name:20} - {percentage:5.2f}% ({class_stats[class_name]:,} шт.)\n"
    
    result += "\n🎪 Топ-10 самых редких классов:\n\n"
    for i, (class_name, percentage) in enumerate(class_percentages.tail(10).items(), 1):
        result += f"{i}. {class_name:20} - {percentage:5.2f}% ({class_stats[class_name]:,} шт.)\n"
    
    await message.answer(result)

# Функция для дополнительной статистики
async def additional_statistics(message: Message):
    result = "📈 Дополнительная статистика:\n\n"
    result += f"📋 Всего записей в датасете: {len(df):,}\n"
    result += f"🧹 Записей после очистки (с массой и координатами): {len(df_clean):,}\n"
    
    result += "\n⚖️ Статистика по массам метеоритов:\n\n"
    result += f"📉 Минимальная масса: {df_clean['mass'].min():.2f} г\n"
    result += f"📈 Максимальная масса: {df_clean['mass'].max():.2f} г\n"
    result += f"📊 Медианная масса: {df_clean['mass'].median():.2f} г\n"
    result += f"📐 Стандартное отклонение: {df_clean['mass'].std():.2f} г\n"
    
    result += "\n🌍 Географическое распределение:\n\n"
    result += f"📍 Минимальная широта: {df_clean['reclat'].min():.2f}°\n"
    result += f"📍 Максимальная широта: {df_clean['reclat'].max():.2f}°\n"
    result += f"📍 Минимальная долгота: {df_clean['reclong'].min():.2f}°\n"
    result += f"📍 Максимальная долгота: {df_clean['reclong'].max():.2f}°"
    
    await message.answer(result)

# Функция для анализа по годам и типам падения
async def year_fall_analysis(message: Message):
    result = ""
    if 'year' in df.columns and 'fall' in df.columns:
        df['year'] = pd.to_numeric(df['year'], errors='coerce')
        
        result = "📅 Дополнительный анализ:\n\n"
        
        # Анализ по типам падения
        fall_stats = df['fall'].value_counts()
        fall_percentages = (fall_stats / len(df) * 100).round(2)
        
        result += "🎯 Распределение по типам обнаружения:\n\n"
        for fall_type, count in fall_stats.items():
            percentage = fall_percentages[fall_type]
            result += f"{fall_type}: {count:,} ({percentage}%)\n"
        
        # Анализ по годам
        result += f"\n📆 Статистика по годам падения:\n\n"
        result += f"🕰 Самый ранний год: {int(df['year'].min())}\n"
        result += f"🕰 Самый поздний год: {int(df['year'].max())}\n"
        result += f"⏱ Средний год: {df['year'].mean():.0f}\n"
        
        # Анализ по десятилетиям
        df['decade'] = (df['year'] // 10) * 10
        decade_stats = df['decade'].value_counts().sort_index()
        
        result += f"\n📊 Количество метеоритов по десятилетиям (первые 10):\n\n"
        count = 0
        for decade, dec_count in decade_stats.items():
            if not pd.isna(decade) and count < 10:
                result += f"{int(decade)}-е: {dec_count:,} метеоритов\n"
                count += 1
    else:
        result = "❌ Данные о годах и типах падения отсутствуют в датасете"
    
    await message.answer(result)

# Функция для топ-5 самых тяжелых метеоритов
async def heavy_meteorites(message: Message):
    result = "🏆 Топ-5 самых тяжелых метеоритов:\n\n"
    
    heavy_meteorites_df = df_clean.nlargest(5, 'mass')[['name', 'mass', 'reclat', 'reclong', class_column]]
    
    for i, (_, row) in enumerate(heavy_meteorites_df.iterrows(), 1):
        result += f"{i}. {row['name']}\n"
        result += f"   ⚖️ Масса: {row['mass']:,.0f} г ({row['mass']/1000:,.1f} кг)\n"
        result += f"   🎯 Класс: {row[class_column]}\n"
        result += f"   📍 Координаты: {row['reclat']:.2f}°, {row['reclong']:.2f}°\n\n"
    
    await message.answer(result)

# Функция для сохранения результатов
async def save_results(message: Message):
    try:
        # Рассчитываем статистику
        average_mass = df_clean['mass'].mean()
        mean_lat = df_clean['reclat'].mean()
        mean_long = df_clean['reclong'].mean()
        
        class_stats = df[class_column].value_counts()
        class_percentages = (class_stats / len(df) * 100).round(2)
        
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
        
        result = "💾 Результаты сохранены в файлы:\n\n"
        result += "📁 meteorite_class_statistics.csv\n"
        result += "📁 meteorite_summary_statistics.csv"
        
        await message.answer(result)
    except Exception as e:
        await message.answer(f"❌ Ошибка при сохранении: {str(e)}")

# ==================== ОБРАБОТЧИКИ КОМАНД ====================

@dp.message(Command("start"))
async def cmd_start(message: Message):
    welcome_text = """
🚀 Добро пожаловать в бот анализа метеоритов!

Я могу показать различные статистики из датасета метеоритов:

📊 Доступные команды:

/preview - Предварительный просмотр данных
/info - Информация о датасете
/avg_mass - Средняя масса метеоритов
/avg_coords - Средние координаты падения
/class_stats - Статистика по классам метеоритов
/additional_stats - Дополнительная статистика
/heavy_meteorites - Топ-5 самых тяжелых метеоритов
/year_analysis - Анализ по годам и типам падения
/save_results - Сохранить результаты в CSV
/all_stats - Полная статистика

Выберите команду из меню ниже 👇
"""
    await message.answer(welcome_text, reply_markup=get_main_keyboard())

@dp.message(Command("preview"))
async def cmd_preview(message: Message):
    await preview_data(message)

@dp.message(Command("info"))
async def cmd_info(message: Message):
    await dataset_info(message)

@dp.message(Command("avg_mass"))
async def cmd_avg_mass(message: Message):
    await average_mass_stat(message)

@dp.message(Command("avg_coords"))
async def cmd_avg_coords(message: Message):
    await average_coordinates(message)

@dp.message(Command("class_stats"))
async def cmd_class_stats(message: Message):
    await class_statistics(message)

@dp.message(Command("additional_stats"))
async def cmd_additional_stats(message: Message):
    await additional_statistics(message)

@dp.message(Command("heavy_meteorites"))
async def cmd_heavy_meteorites(message: Message):
    await heavy_meteorites(message)

@dp.message(Command("year_analysis"))
async def cmd_year_analysis(message: Message):
    await year_fall_analysis(message)

@dp.message(Command("save_results"))
async def cmd_save_results(message: Message):
    await save_results(message)

@dp.message(Command("all_stats"))
async def cmd_all_stats(message: Message):
    await message.answer("📊 Загружаю полную статистику...")
    await preview_data(message)
    await asyncio.sleep(0.5)
    await average_mass_stat(message)
    await asyncio.sleep(0.5)
    await average_coordinates(message)
    await asyncio.sleep(0.5)
    await class_statistics(message)
    await asyncio.sleep(0.5)
    await additional_statistics(message)
    await asyncio.sleep(0.5)
    await year_fall_analysis(message)
    await asyncio.sleep(0.5)
    await heavy_meteorites(message)

# Обработчик для текстовых сообщений
@dp.message()
async def handle_text(message: Message):
    text = message.text.lower()
    if "масса" in text:
        await average_mass_stat(message)
    elif "координат" in text or "точк" in text:
        await average_coordinates(message)
    elif "класс" in text or "тип" in text:
        await class_statistics(message)
    elif "тяжел" in text or "больш" in text:
        await heavy_meteorites(message)
    elif "статистик" in text:
        await additional_statistics(message)
    else:
        await message.answer("🤔 Не понял ваш запрос. Используйте команды из меню ниже 👇", 
                           reply_markup=get_main_keyboard())

# ==================== ЗАПУСК БОТА ====================

async def main():
    print("🚀 Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":

    asyncio.run(main())
