package com.example.demo;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import lombok.RequiredArgsConstructor;

@Service 
@RequiredArgsConstructor 
public class PredictService {
    private final CopyOnWriteArrayList<SseEmitter> emitters = new CopyOnWriteArrayList<>();

    @Value("${fastapi.url}")
    private String fastApiUrl;

    WebClient webClient = WebClient.builder().build();

    public SseEmitter subscribe(){
        SseEmitter emitter = new SseEmitter();
        emitters.add(emitter);
        emitter.onCompletion(() -> emitters.remove(emitter));
        try {
            emitter.send(SseEmitter.event().name("connect").data("연결 완료"));
        } catch (IOException e) {
            emitters.remove(emitter);
        }
        return emitter;
    }

    public void onMessage(String name, Object data){
        for(SseEmitter emitter : emitters){
            try{
                emitter.send(SseEmitter.event().name(name).data(data));
            }catch(IOException e){
                emitters.remove(emitter);
            }
        }
    }
    
    public void predict(PredictDto dto){
        webClient.post()
            .uri(fastApiUrl+"/api/predict")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(dto)
            .retrieve()
            .bodyToFlux(Map.class)
            .subscribe(
                result -> onMessage("result", result),
                error -> onMessage("fail", "AI 서버 오류")
            );
    }
}   
