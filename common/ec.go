/*
@Time : 2021/12/10 12:37
@Author : Dao
@File : ec.go
@Software: GoLand
*/

package common

import "errors"

var (
	ErrInvalidRecord = errors.New("model/record: invalid key or value")
)
