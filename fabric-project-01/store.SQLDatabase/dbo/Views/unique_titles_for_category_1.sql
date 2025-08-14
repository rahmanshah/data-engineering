CREATE VIEW [dbo].[unique_titles_for_category_1] AS SELECT COUNT(DISTINCT title) AS unique_titles_for_category_1 FROM products WHERE category=1

GO

