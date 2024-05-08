class Telephone :
    def __init__(self, model,costperhour):
        self.model=model
        self.costperhour=costperhour
    def installationcost(self,hoursforinstallation):
        return hoursforinstallation*self.costperhour