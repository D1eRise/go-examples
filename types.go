package controllers

import (
	"encoding/json"
	"errors"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"gitlab.infospice.ru/sensorium/sensorium/pkg/app/entity"
	"gitlab.infospice.ru/sensorium/sensorium/pkg/app/entity/api"
	"gitlab.infospice.ru/sensorium/sensorium/pkg/app/entity/sensor"
	"gitlab.infospice.ru/sensorium/sensorium/pkg/app/helpers/tools"
	userRep "gitlab.infospice.ru/sensorium/sensorium/pkg/app/repository/user"
	"golang.org/x/exp/slices"
	"gorm.io/gorm"
	"net/http"
	"reflect"
	"time"
)

type editSensorTypeRequest struct {
	Id     uuid.UUID                         `json:"id"`
	Fields map[string]sensor.JsonParamsField `json:"fields"`
}

// EditTypes
// @BasePath	/types/
// @Router		/types/ [put]
// @Summary		Изменить типы (дефолтные значения параметров)
// @Description Изменить типы (дефолтные значения параметров)
// @Tags		Types
// @Accept		json
// @Produce		json
// @Param request body []editSensorTypeRequest true "Body"
// @Param		Authorization		header		string			true	"Authorization token"
// @Success		200		{object}	api.Response
func EditTypes(db *gorm.DB) gin.HandlerFunc {

	return func(c *gin.Context) {

		r := api.NewResponse()

		token := c.GetHeader("Authorization")
		user := userRep.GetByToken(db, token)

		if !user.IsAuth() {
			c.JSON(http.StatusForbidden, r.AddError(errors.New("пользователь не авторизован")))
			return
		}

		var requestFields []editSensorTypeRequest
		if err := c.ShouldBindJSON(&requestFields); err != nil {
			c.JSON(http.StatusBadRequest, r.AddError(err))
			return
		}

		if len(requestFields) < 1 {
			c.JSON(http.StatusBadRequest, r.AddError(errors.New("пустой запрос")))
			return
		}

		var typesIds []uuid.UUID

		for _, t := range requestFields {
			typesIds = append(typesIds, t.Id)
		}

		var types []entity.SensorType

		err := db.Where("id IN ?", typesIds).Find(&types).Error
		if err != nil {
			c.JSON(http.StatusBadRequest, r.AddError(err))
			return
		}

		if len(types) < len(typesIds) {
			c.JSON(http.StatusBadRequest, r.AddError(errors.New("переданы несуществующие типы")))
			return
		}

		var createBatch []entity.SensorType

		for tIdx := range types {

			idx := slices.IndexFunc(requestFields, func(rf editSensorTypeRequest) bool { return rf.Id == types[tIdx].Id })
			if idx == -1 {
				continue
			}

			if len(requestFields[idx].Fields) < 1 {
				continue
			}

			ss, _ := json.Marshal(types[tIdx].Params)
			var params sensor.JsonParams
			_ = json.Unmarshal(ss, &params)

			paramsUpdated := false

			for keyField := range params.Fields {
				if _, exists := requestFields[idx].Fields[keyField]; exists {
					if params.Fields[keyField].Default == requestFields[idx].Fields[keyField].Default {
						continue
					}
					field := params.Fields[keyField]
					field.Default = requestFields[idx].Fields[keyField].Default
					field.Value = requestFields[idx].Fields[keyField].Default
					params.Fields[keyField] = field
					if !paramsUpdated {
						paramsUpdated = true
					}
				}
			}

			if paramsUpdated {
				p, _ := tools.StructToMap(params)
				types[tIdx].Params = p
				types[tIdx].InsertTime = time.Now()
				createBatch = append(createBatch, types[tIdx])
			}
		}

		if len(createBatch) > 0 {
			err = db.Create(createBatch).Error
			if err != nil {
				c.JSON(http.StatusBadRequest, r.AddError(err))
				return
			}
			db.Exec("OPTIMIZE TABLE sensor_types FINAL;")
		}

		c.JSON(http.StatusOK, r.SetData(types))
	}
}

// UpdateSensorsByType
// @BasePath	/types/update-sensors/{id}
// @Router		/types/update-sensors/{id} [put]
// @Summary		Установить сенсорам значения параметров из типа
// @Description Установить сенсорам значения параметров из типа
// @Tags		Types
// @Accept		json
// @Produce		json
// @Param		id	path	string	true	"Type ID"
// @Param		Authorization		header		string			true	"Authorization token"
// @Success		200		{object}	api.Response
func UpdateSensorsByType(db *gorm.DB) gin.HandlerFunc {

	return func(c *gin.Context) {

		r := api.NewResponse()

		token := c.GetHeader("Authorization")
		user := userRep.GetByToken(db, token)

		if !user.IsAuth() {
			c.JSON(http.StatusForbidden, r.AddError(errors.New("пользователь не авторизован")))
			return
		}

		strId := c.Param("id")
		typeId, err := uuid.Parse(strId)

		if err != nil {
			c.JSON(http.StatusBadRequest, r.AddError(err))
			return
		}

		var sensorType entity.Sensor

		if err = db.Find(&sensorType, entity.SensorType{Id: typeId}).Error; err != nil {
			if err != nil {
				c.JSON(http.StatusBadRequest, r.AddError(err))
				return
			}
		}

		if sensorType.Id == uuid.Nil {
			c.JSON(http.StatusBadRequest, r.AddError(errors.New("тип не существует")))
			return
		}

		var sensors []entity.Sensor

		if err = db.Find(&sensors, entity.Sensor{TypeId: typeId}).Error; err != nil {
			c.JSON(http.StatusBadRequest, r.AddError(err))
			return
		}

		var createBatch []entity.Sensor

		for sIdx := range sensors {
			if reflect.DeepEqual(sensors[sIdx].Params, sensorType.Params) {
				continue
			}
			sensors[sIdx].Params = sensorType.Params
			sensors[sIdx].InsertTime = time.Now()
			createBatch = append(createBatch, sensors[sIdx])
		}

		if len(createBatch) > 0 {
			err = db.Create(createBatch).Error
			if err != nil {
				c.JSON(http.StatusBadRequest, r.AddError(err))
				return
			}
			db.Exec("OPTIMIZE TABLE sensors FINAL;")
		}

		c.JSON(http.StatusOK, r.SetData(sensors))
	}
}
