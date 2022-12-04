package samples

// [START spanner_associations]

import (
	"context"
	"database/sql"
	"fmt"
	"gorm.io/gorm"
	"io"
	"time"

	_ "github.com/googleapis/go-sql-spanner"

	spannergorm "github.com/cloudspannerecosystem/go-spanner-orm/gorm"
)

func Association(ctx context.Context, w io.Writer, dsn string) error {
	// dsn := "projects/my-project/instances/my-instance/databases/my-database"
	db, err := gorm.Open(spannergorm.New(spannergorm.Config{
		DriverName: "spanner",
		DSN:        dsn,
	}), &gorm.Config{PrepareStmt: true})
	if err != nil {
		return err
	}

	// Has one
	type CreditCard struct {
		ID        int `gorm:"primarykey;autoIncrement:false"`
		CreatedAt time.Time
		UpdatedAt time.Time
		Number    string
		UserID    int
	}
	// Many2Many
	type Language struct {
		ID        int `gorm:"primarykey;autoIncrement:false"`
		CreatedAt time.Time
		UpdatedAt time.Time
		DeletedAt sql.NullTime `gorm:"index"`
		Name      string
	}
	// User has one CreditCard, UserID is the foreign key
	type User struct {
		ID         int `gorm:"primarykey;autoIncrement:false"`
		CreatedAt  time.Time
		UpdatedAt  time.Time
		DeletedAt  sql.NullTime `gorm:"index"`
		CreditCard CreditCard
		Languages  []Language `gorm:"many2many:user_languages;"`
	}
	// Composite Foreign Keys
	type Tag struct {
		ID     uint   `gorm:"primaryKey"`
		Locale string `gorm:"primaryKey"`
		Value  string
	}

	type Blog struct {
		ID         uint   `gorm:"primaryKey"`
		Locale     string `gorm:"primaryKey"`
		Subject    string
		Body       string
		Tags       []Tag `gorm:"many2many:blog_tags;"`
		LocaleTags []Tag `gorm:"many2many:locale_blog_tags;ForeignKey:id,locale;References:id"`
		SharedTags []Tag `gorm:"many2many:shared_blog_tags;ForeignKey:id;References:id"`
	}

	// Join Table: blog_tags
	//   foreign key: blog_id, reference: blogs.id
	//   foreign key: blog_locale, reference: blogs.locale
	//   foreign key: tag_id, reference: tags.id
	//   foreign key: tag_locale, reference: tags.locale

	// Join Table: locale_blog_tags
	//   foreign key: blog_id, reference: blogs.id
	//   foreign key: blog_locale, reference: blogs.locale
	//   foreign key: tag_id, reference: tags.id

	// Join Table: shared_blog_tags
	//   foreign key: blog_id, reference: blogs.id
	//   foreign key: tag_id, reference: tags.id

	db.AutoMigrate(&User{}, &CreditCard{}, &Language{}, &Tag{}, &Blog{})

	db.Create(&User{
		ID:         1,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
		DeletedAt:  sql.NullTime{Valid: false},
		CreditCard: CreditCard{ID: 1, CreatedAt: time.Now(), UpdatedAt: time.Now(), Number: "1"},
	})
	db.Create(&User{
		ID:         2,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
		DeletedAt:  sql.NullTime{Valid: false},
		CreditCard: CreditCard{ID: 2, CreatedAt: time.Now(), UpdatedAt: time.Now(), Number: "2"},
	})
	db.Create(&User{
		ID:         3,
		CreatedAt:  time.Now(),
		UpdatedAt:  time.Now(),
		DeletedAt:  sql.NullTime{Valid: false},
		CreditCard: CreditCard{ID: 3, CreatedAt: time.Now(), UpdatedAt: time.Now(), Number: "3"},
	})
	getAll := func(db *gorm.DB) ([]User, error) {
		var users []User
		err := db.Model(&User{}).Preload("CreditCard").Find(&users).Error
		return users, err
	}
	all, err := getAll(db)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "returns users %+v.\n", all)
	return err
}

// [END spanner_associations]
