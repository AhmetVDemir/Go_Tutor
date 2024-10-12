package persistance

import (
	"context"
	"mpapp/domain"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/labstack/gommon/log"
)

type IProductRepository interface {
	GetAllProducts() []domain.Product
	GetAllProductsByStore(storeName string) []domain.Product
	AddProduct(product domain.Product) error
}

type ProductRepository struct {
	dbPool *pgxpool.Pool
}

// Go newletemez elle new çağrısı yaptık
func NewProductRepository(dbPool *pgxpool.Pool) IProductRepository {
	return &ProductRepository{
		dbPool: dbPool,
	}
}

func (productRepository *ProductRepository) GetAllProducts() []domain.Product {

	ctx := context.Background()
	productRows, err := productRepository.dbPool.Query(ctx, "Select * from products")

	if err != nil {
		log.Error("Ürünler getirilirken hata oluştu ! %v", err)
		return []domain.Product{}
	}

	return extractProductsFromRows(productRows)
}

func (productRepository *ProductRepository) GetAllProductsByStore(storeName string) []domain.Product {

	ctx := context.Background()

	getProductsByStoreNameSql := `Select * from products where store =$1`

	productRows, err := productRepository.dbPool.Query(ctx, getProductsByStoreNameSql, storeName)

	if err != nil {
		log.Error("Ürünler getirilirken hata oluştu ! %v", err)
		return []domain.Product{}
	}

	return extractProductsFromRows(productRows)
}

func (productRepository *ProductRepository) AddProduct(produtc domain.Product) error {
	ctx := context.Background()
	insert_sql := `Insert into products(name,price,discount,store) VALUES ($1,$2,$3,$4)`
	addNewProduct, err := productRepository.dbPool.Exec(ctx, insert_sql, produtc.Name, produtc.Price, produtc.Discount, produtc.Store)

	if err != nil {
		log.Error("Ürün eklenirken hata oluştu", err)
		return err
	}
	log.Info("Ürün eklendi : %v", addNewProduct)
	return nil

}

func extractProductsFromRows(productRows pgx.Rows) []domain.Product {
	var products = []domain.Product{}
	var id int64
	var name string
	var price float32
	var discount float32
	var store string

	for productRows.Next() {
		productRows.Scan(&id, &name, &price, &discount, &store)
		products = append(products, domain.Product{
			Id:       id,
			Name:     name,
			Price:    price,
			Discount: discount,
			Store:    store,
		})
	}

	return products
}
