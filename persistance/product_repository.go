package persistance

import (
	"context"
	"errors"
	"fmt"
	"mpapp/domain"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/labstack/gommon/log"
)

type IProductRepository interface {
	GetAllProducts() []domain.Product
	GetAllProductsByStore(storeName string) []domain.Product
	AddProduct(product domain.Product) error
	GetProductById(productId int64) (domain.Product, error)
	DeleteById(productId int64) error
	UpdatePrice(productId int64, newPrice float32) error
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

func (productRepository *ProductRepository) GetProductById(productId int64) (domain.Product, error) {
	ctx := context.Background()

	getByIdSql := `Select * from products where id = $1`
	queryRow := productRepository.dbPool.QueryRow(ctx, getByIdSql, productId)
	var id int64
	var name string
	var price float32
	var discount float32
	var store string

	scanErr := queryRow.Scan(&id, &name, &price, &discount, &store)

	if scanErr != nil {
		return domain.Product{}, errors.New(fmt.Sprintf("Id ye göre ürün getirilirken hata ile karşılaşıldı : %d ", productId))
	}

	return domain.Product{
		Id:       id,
		Name:     name,
		Price:    price,
		Discount: discount,
		Store:    store,
	}, nil

}

func (productRepository *ProductRepository) DeleteById(productId int64) error {

	ctx := context.Background()

	_, getErr := productRepository.GetProductById(productId)
	if getErr != nil {

		return errors.New("Ürün bulunamadı !")

	}

	deleteSQL := `Delete from products where id = $1`
	_, err := productRepository.dbPool.Exec(ctx, deleteSQL, productId)
	if err != nil {
		return errors.New(fmt.Sprintf("Silme işlemi sırasında hata oluştu : %d", productId))
	}
	log.Info("Ürün silindi !")
	return nil

}

func (productRepository *ProductRepository) UpdatePrice(productId int64, newPrice float32) error {
	ctx := context.Background()

	updateSQL := `Update products set price = $1 where id = $2`
	_, err := productRepository.dbPool.Exec(ctx, updateSQL, newPrice, productId)
	if err != nil {
		return errors.New(fmt.Sprintf("Güncelleme sırasında hata oldu : %d", productId))
	}
	log.Info(" %d Ürün Güncelleme başarılı  ! %v ", productId, newPrice)
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
