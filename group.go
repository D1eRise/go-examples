package group

import (
	"errors"
	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"
	"gitlab.infospice.ru/sensorium/sensorium/pkg/app/entity"
	"gitlab.infospice.ru/sensorium/sensorium/pkg/app/helpers/database"
	"golang.org/x/exp/slices"
	"gorm.io/gorm"
	"sync"
)

// GroupsSingle - создание, изменение, удаление группы

type GroupsSingle struct {
	listBatch   []entity.Group
	createBatch []entity.Group
	mu          sync.Mutex
	db          *gorm.DB
}

type Tree struct {
	Id   uuid.UUID `json:"id"`
	Name string    `json:"name"`
}

type TreeBottomElement struct {
	Id uuid.UUID `json:"id"`
}

var (
	instance *GroupsSingle
	once     sync.Once
)

func GetInstance() *GroupsSingle {
	once.Do(
		func() {
			var groups []entity.Group
			db := database.GetInstance().GetConnection()
			err := db.Order("depth_level").Find(&groups).Error
			if err != nil {
				log.Fatal(err)
			}
			instance = &GroupsSingle{listBatch: groups, createBatch: []entity.Group{}, db: db}
		})
	return instance
}

func (GroupsSingle *GroupsSingle) Lock() {
	GroupsSingle.mu.Lock()
}

func (GroupsSingle *GroupsSingle) Unlock() {
	GroupsSingle.mu.Unlock()
}

func (GroupsSingle *GroupsSingle) Reload() {
	GroupsSingle.db.Exec("OPTIMIZE TABLE groups FINAL;")
	var groups []entity.Group
	err := GroupsSingle.db.Order("depth_level").Find(&groups).Error
	if err != nil {
		log.Fatal(err)
	}
	GroupsSingle.mu.Lock()
	GroupsSingle.listBatch = groups
	GroupsSingle.mu.Unlock()
}

func (GroupsSingle *GroupsSingle) CreateFromBatch(optimize bool) error {

	GroupsSingle.mu.Lock()

	if len(GroupsSingle.createBatch) < 1 {
		return nil
	}

	err := GroupsSingle.db.Create(GroupsSingle.createBatch).Error
	if err != nil {
		var groups []entity.Group
		innerErr := GroupsSingle.db.Order("depth_level").Find(&groups).Error
		if innerErr != nil {
			log.Fatal(innerErr)
		}
		GroupsSingle.listBatch = groups
		GroupsSingle.createBatch = []entity.Group{}
		return err
	}

	if optimize {
		GroupsSingle.db.Exec("OPTIMIZE TABLE groups FINAL;")
	}

	GroupsSingle.createBatch = []entity.Group{}

	GroupsSingle.mu.Unlock()

	return nil
}

func (GroupsSingle *GroupsSingle) DeleteById(id uuid.UUID) error {

	idx := slices.IndexFunc(GroupsSingle.listBatch, func(g entity.Group) bool { return g.Id == id })
	if idx == -1 {
		return errors.New("удаляемая группа не найдена")
	}

	GroupsSingle.mu.Lock()
	err := GroupsSingle.db.Where("id = ?", id).Delete(&entity.Group{}).Error
	if err != nil {
		return err
	}
	GroupsSingle.listBatch = append(GroupsSingle.listBatch[:idx], GroupsSingle.listBatch[idx+1:]...)
	GroupsSingle.mu.Unlock()

	return nil
}

func (GroupsSingle *GroupsSingle) ResetGroupsTable() error {

	GroupsSingle.mu.Lock()

	err := GroupsSingle.db.Where("main = ?", false).Delete(&entity.Group{}).Error
	if err != nil {
		return err
	}

	for idx := range GroupsSingle.listBatch {
		if !GroupsSingle.listBatch[idx].Main {
			GroupsSingle.listBatch = append(GroupsSingle.listBatch[:idx], GroupsSingle.listBatch[idx+1:]...)
		}
	}

	GroupsSingle.mu.Unlock()

	return nil
}

func (GroupsSingle *GroupsSingle) GetBatch() []entity.Group {
	return GroupsSingle.listBatch
}

func (GroupsSingle *GroupsSingle) GetBatchElementById(id uuid.UUID) entity.Group {
	idx := slices.IndexFunc(GroupsSingle.listBatch, func(g entity.Group) bool { return g.Id == id })
	if idx == -1 {
		return entity.Group{}
	}
	return GroupsSingle.listBatch[idx]
}

func (GroupsSingle *GroupsSingle) GetBatchElementsByParentId(id uuid.UUID) []entity.Group {
	var groups []entity.Group
	for _, g := range GroupsSingle.listBatch {
		if g.ParentId == id {
			groups = append(groups, g)
		}
	}
	return groups
}

func (GroupsSingle *GroupsSingle) GetMainGroupUuid() (uuid.UUID, error) {

	idx := slices.IndexFunc(GroupsSingle.listBatch, func(g entity.Group) bool { return g.Main })
	if idx == -1 {
		err := errors.New("нарушена структура групп, главная группа не найдена")
		return uuid.Nil, err
	}
	return GroupsSingle.listBatch[idx].Id, nil
}

func (GroupsSingle *GroupsSingle) GetOfficeGroupUuid(office entity.Office) (uuid.UUID, error) {

	idx := slices.IndexFunc(GroupsSingle.listBatch, func(g entity.Group) bool {
		return g.OfficeGroup && g.Office == office.Id
	})
	if idx == -1 {
		err := errors.New("нарушена структура групп, группа офиса не найдена (офис " + office.Name + ")")
		return uuid.Nil, err
	}
	return GroupsSingle.listBatch[idx].Id, nil
}

func (GroupsSingle *GroupsSingle) UpdateBatch(group entity.Group) {

	GroupsSingle.mu.Lock()

	idx := slices.IndexFunc(GroupsSingle.listBatch, func(g entity.Group) bool { return g.Id == group.Id })
	if idx == -1 {
		GroupsSingle.listBatch = append(GroupsSingle.listBatch, group)
	} else {
		GroupsSingle.listBatch[idx] = group
	}

	idx = slices.IndexFunc(GroupsSingle.createBatch, func(g entity.Group) bool { return g.Id == group.Id })
	if idx == -1 {
		GroupsSingle.createBatch = append(GroupsSingle.createBatch, group)
	} else {
		GroupsSingle.createBatch[idx] = group
	}

	GroupsSingle.mu.Unlock()
}

func (GroupsSingle *GroupsSingle) RecalcGroupsDepth(rootGroupId uuid.UUID) {

	rootGroup := GroupsSingle.GetBatchElementById(rootGroupId)
	GroupsSingle.setGroupDepth(rootGroup.Id, rootGroup.DepthLevel+1)
}

func (GroupsSingle *GroupsSingle) setGroupDepth(parent uuid.UUID, depth int32) {

	for idx := range GroupsSingle.listBatch {

		if GroupsSingle.listBatch[idx].ParentId == parent {

			if GroupsSingle.listBatch[idx].DepthLevel != depth {

				GroupsSingle.mu.Lock()

				GroupsSingle.listBatch[idx].DepthLevel = depth

				cidx := slices.IndexFunc(GroupsSingle.createBatch, func(g entity.Group) bool { return g.Id == GroupsSingle.listBatch[idx].Id })
				if cidx == -1 {
					GroupsSingle.createBatch = append(GroupsSingle.createBatch, GroupsSingle.listBatch[idx])
				} else {
					GroupsSingle.createBatch[cidx].DepthLevel = depth
				}

				GroupsSingle.mu.Unlock()
			}

			GroupsSingle.setGroupDepth(GroupsSingle.listBatch[idx].Id, depth+1)
		}
	}
}

func (GroupsSingle *GroupsSingle) CheckParentId(id uuid.UUID, idToCheck uuid.UUID, forRelink bool) string {

	var checkErr string

	if forRelink {
		mainGroupId, err := GroupsSingle.GetMainGroupUuid()
		if err != nil {
			return err.Error()
		}
		checkErr = "Ошибка переноса подгрупп, сети и устройств группы"
		if idToCheck == mainGroupId {
			return checkErr + " (перенос в главную группу невозможен)"
		}
	} else {
		checkErr = "Ошибка изменения родительской группы"
	}

	if id == idToCheck {
		if forRelink {
			checkErr += " (перенос в удаляемую или деактивируемую группу невозможен)"
		} else {
			checkErr += " (изменяемая группа не может быть собственным родителем)"
		}
		return checkErr
	}

	idx := slices.IndexFunc(GroupsSingle.listBatch, func(g entity.Group) bool { return g.Id == idToCheck && g.Active })
	if idx == -1 {
		return checkErr + " (группа не существует или неактивна)"
	}

	treeBottomElements := GroupsSingle.getTreeBottomElements(id)

	idx = slices.IndexFunc(treeBottomElements, func(g TreeBottomElement) bool { return g.Id == idToCheck })
	if idx != -1 {
		if forRelink {
			checkErr += " (перенос в подгруппы удаляемой или деактивируемой группы невозможен)"
		} else {
			checkErr += " (подгруппа изменяемой группы не может быть её родителем)"
		}
		return checkErr
	}

	return ""
}

func (GroupsSingle *GroupsSingle) GetTreeTop(groupId uuid.UUID, deviceId uuid.UUID, deviceName string, deviceHost string) []Tree {

	tree := make([]Tree, 0)

	group := GroupsSingle.GetBatchElementById(groupId)

	if group.Id == uuid.Nil {
		return tree
	}

	tree = []Tree{
		{
			Id:   groupId,
			Name: group.Name,
		},
		{
			Id:   deviceId,
			Name: deviceName + " (" + deviceHost + ")",
		},
	}

	GroupsSingle.CompileTreeTop(&tree, group.ParentId)

	return tree
}

func (GroupsSingle *GroupsSingle) CompileTreeTop(tree *[]Tree, parentId uuid.UUID) {

	if parentId == uuid.Nil {
		return
	}

	group := GroupsSingle.GetBatchElementById(parentId)

	if group.Id == uuid.Nil {
		return
	}

	newTreeEl := Tree{
		Id:   group.Id,
		Name: group.Name,
	}
	*tree = append([]Tree{newTreeEl}, *tree...)

	GroupsSingle.CompileTreeTop(tree, group.ParentId)
}

func (GroupsSingle *GroupsSingle) getTreeBottomElements(groupId uuid.UUID) []TreeBottomElement {

	treeElements := make([]TreeBottomElement, 0)
	groups := GroupsSingle.GetBatchElementsByParentId(groupId)

	if len(groups) == 0 {
		return treeElements
	}

	for _, group := range groups {
		treeElements = append(treeElements, TreeBottomElement{
			Id: group.Id,
		})
		GroupsSingle.compileTreeBottomElements(&treeElements, group.Id)
	}

	return treeElements
}

func (GroupsSingle *GroupsSingle) compileTreeBottomElements(treeElements *[]TreeBottomElement, groupId uuid.UUID) {

	if groupId == uuid.Nil {
		return
	}

	groups := GroupsSingle.GetBatchElementsByParentId(groupId)

	if len(groups) == 0 {
		return
	}

	for _, group := range groups {
		*treeElements = append(*treeElements, TreeBottomElement{
			Id: group.Id,
		})
		GroupsSingle.compileTreeBottomElements(treeElements, group.Id)
	}
}
