library(sf)
library(skimr)

# Use the dataset directly from the package namespace
idealista18::Barcelona_Sale |>
  sf::st_drop_geometry() |> 
  skimr::skim()

df <- idealista18::Barcelona_Sale |> sf::st_drop_geometry()
write.csv(df, "Barcelona_Sale.csv", row.names = FALSE)
