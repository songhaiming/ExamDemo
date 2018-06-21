package com.migu.schedule;


import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/*
*类名和方法不能修改
 */
public class Schedule {

    public Map nodeMap = new HashMap<String,String>();
    public Map taskMap = new HashMap<String,TaskInfo>();
    public Map nodeMapTask = new HashMap<String,List<Map>>();
    public List<Map> taskQueue = new ArrayList<Map>();

    public int init() {
        nodeMap.clear();
        taskMap.clear();
        nodeMapTask.clear();
        taskQueue.clear();
        return ReturnCodeKeys.E001;
    }


    public int registerNode(int nodeId) {
        if(nodeId <= 0){
            return ReturnCodeKeys.E004;
        }
        if (nodeMap.get(String.valueOf(nodeId)) != null){
            return ReturnCodeKeys.E005;
        }
        nodeMap.put(String.valueOf(nodeId),String.valueOf(nodeId));
        return ReturnCodeKeys.E003;
    }

    public int unregisterNode(int nodeId) {
        if(nodeId <= 0 ){
            return ReturnCodeKeys.E004;
        }
        if(nodeMap.get(String.valueOf(nodeId)) == null){
            return ReturnCodeKeys.E007;
        }
        nodeMap.remove(String.valueOf(nodeId));
        if(nodeMapTask.get(String.valueOf(nodeId)) != null){
            List<Map> list = (List<Map>)nodeMapTask.get(String.valueOf(nodeId));
            for(Map map:list){
                taskQueue.add(map);
            }
        }
        nodeMapTask.remove(String.valueOf(nodeId));
        return ReturnCodeKeys.E006;
    }


    public int addTask(int taskId, int consumption) {
        if(taskId <= 0){
            return ReturnCodeKeys.E009;
        }
        if(taskMap.get(String.valueOf(taskId)) != null){
            return ReturnCodeKeys.E010;
        }
        TaskInfo taskInfo = new TaskInfo();
        taskInfo.setTaskId(taskId);
        taskMap.put(String.valueOf(taskId),taskInfo);
        return ReturnCodeKeys.E008;
    }


    public int deleteTask(int taskId) {
        if(taskId <= 0){
            return ReturnCodeKeys.E009;
        }
        if(taskMap.get(String.valueOf(taskId)) == null)
            return ReturnCodeKeys.E012;
        taskMap.remove(String.valueOf(taskId));
        return ReturnCodeKeys.E011;
    }


    public int scheduleTask(int threshold) {
        return ReturnCodeKeys.E000;
    }


    public int queryTaskStatus(List<TaskInfo> tasks) {
        return ReturnCodeKeys.E000;
    }

}
